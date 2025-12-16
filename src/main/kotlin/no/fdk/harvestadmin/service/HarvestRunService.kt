package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.model.HarvestCurrentState
import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.model.PhaseDurations
import no.fdk.harvestadmin.model.ResourceCounts
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.PageRequest
import org.springframework.http.HttpStatus
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.temporal.ChronoUnit

@Service
class HarvestRunService(
    private val harvestEventRepository: HarvestEventRepository,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
    @Value("\${app.harvest.stale-timeout-minutes:30}") private val staleTimeoutMinutes: Long,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun persistEvent(event: HarvestEvent) {
        try {
            val runId = event.runId?.toString()
            if (runId == null) {
                logger.warn("Cannot process harvest event: runId is required. phase=${event.phase}")
                return
            }

            val currentRun = harvestRunRepository.findByRunId(runId)

            // Use dataSourceId from event if available (INITIATING phase), otherwise from the found run
            val effectiveDataSourceId = event.dataSourceId?.toString() ?: currentRun?.dataSourceId
            if (effectiveDataSourceId == null) {
                logger.warn("Cannot process harvest event: no dataSourceId available. phase=${event.phase}, runId=$runId")
                return
            }
            val dataType = event.dataType.name

            // Check if this resource has already been processed for this phase (for logging only)
            // We no longer persist isDuplicate - we use the latest event approach instead
            val isDuplicate =
                runId.let { runIdValue ->
                    when {
                        event.fdkId != null -> {
                            harvestEventRepository.existsByRunIdAndEventTypeAndFdkId(
                                runIdValue,
                                event.phase.name,
                                event.fdkId.toString(),
                            )
                        }
                        event.resourceUri != null -> {
                            harvestEventRepository.existsByRunIdAndEventTypeAndResourceUri(
                                runIdValue,
                                event.phase.name,
                                event.resourceUri.toString(),
                            )
                        }
                        else -> false
                    }
                } ?: false

            if (isDuplicate) {
                logger.debug(
                    "Duplicate event detected for phase=${event.phase}, runId=$runId, fdkId=${event.fdkId}, resourceUri=${event.resourceUri}. Using latest event approach.",
                )
            }

            val entity =
                HarvestEventEntity(
                    eventType = event.phase.name,
                    dataSourceId = effectiveDataSourceId,
                    runId = runId,
                    dataType = dataType,
                    dataSourceUrl = event.dataSourceUrl?.toString(),
                    acceptHeader = event.acceptHeader?.toString(),
                    fdkId = event.fdkId?.toString(),
                    resourceUri = event.resourceUri?.toString(),
                    startTime = event.startTime?.toString(),
                    endTime = event.endTime?.toString(),
                    errorMessage = event.errorMessage?.toString(),
                    changedResourcesCount = event.changedResourcesCount?.let { it.toInt() },
                    unchangedResourcesCount = event.unchangedResourcesCount?.let { it.toInt() },
                    removedResourcesCount = event.removedResourcesCount?.let { it.toInt() },
                )
            harvestEventRepository.save(entity)

            // Update or create harvest run (consolidated state tracking)
            // Only update run if it exists
            if (currentRun != null) {
                updateHarvestRun(event)
            } else {
                logger.debug("Skipping run update: harvest run not found for runId: $runId")
            }

            logger.debug("Persisted harvest event: phase=${event.phase}, rundId=${event.runId}, fdkId=${event.fdkId}")
        } catch (e: Exception) {
            logger.error("Error persisting harvest event: phase=${event.phase}, runId=${event.runId}", e)
            throw e
        }
    }

    private fun updateHarvestRun(event: HarvestEvent) {
        val runId = event.runId?.toString()
        if (runId == null) {
            logger.warn("Cannot update harvest run: runId is required. phase=${event.phase}")
            return
        }

        val currentRun = harvestRunRepository.findByRunId(runId)
        if (currentRun == null) {
            logger.warn("Cannot find harvest run with runId: $runId, phase=${event.phase}")
            return
        }

        if (currentRun != null) {
            val oldStatus = currentRun.status
            val updatedRun = updateRunWithEvent(currentRun, event)
            val savedRun = harvestRunRepository.save(updatedRun)

            // Only record ongoing metrics if the run was still IN_PROGRESS when the event arrived
            // This prevents recording metrics for late-arriving events after a run has completed
            if (oldStatus == "IN_PROGRESS") {
                // Record phase duration if this phase just completed (has endTime)
                val eventStartTime = event.startTime?.let { parseDateTime(it) }
                val eventEndTime = event.endTime?.let { parseDateTime(it) }
                if (eventStartTime != null && eventEndTime != null) {
                    val phaseDurationMs = ChronoUnit.MILLIS.between(eventStartTime, eventEndTime)
                    harvestMetricsService.recordPhaseDurationDuringRun(
                        event.phase.name,
                        phaseDurationMs,
                        savedRun.dataType,
                    )
                }

                // Record resources processed if applicable
                if (savedRun.processedResources != null && savedRun.processedResources > 0) {
                    val resourceProcessingPhases =
                        listOf(
                            "REASONING",
                            "RDF_PARSING",
                            "RESOURCE_PROCESSING",
                            "SEARCH_PROCESSING",
                            "AI_SEARCH_PROCESSING",
                            "SPARQL_PROCESSING",
                        )
                    if (event.phase.name in resourceProcessingPhases) {
                        harvestMetricsService.recordResourcesProcessed(
                            savedRun.dataType,
                            event.phase.name,
                            1,
                        )
                    }
                }

                // Record resource counts during run (not just at completion)
                // Record whenever we have resource data (total or processed)
                if ((savedRun.totalResources != null && savedRun.totalResources > 0) ||
                    (savedRun.processedResources != null && savedRun.processedResources > 0)
                ) {
                    harvestMetricsService.recordRunResourceCounts(savedRun)
                }
            }

            // Record metrics if status changed (always record completion/failure metrics)
            if (oldStatus != savedRun.status) {
                harvestMetricsService.recordRunCompleted(savedRun)
            }
        }
    }

    private fun updateRunWithEvent(
        run: HarvestRunEntity,
        event: HarvestEvent,
    ): HarvestRunEntity {
        val startTime = event.startTime?.let { parseDateTime(it) }
        val endTime = event.endTime?.let { parseDateTime(it) }
        val currentPhase = event.phase.name
        // Use startTime if available, otherwise fallback to runStartedAt for timestamp
        val eventTimestamp = startTime ?: run.runStartedAt

        // Calculate resource counts
        val totalResources = calculateTotalResources(event, run)
        val processedResources = calculateProcessedResources(event, run, totalResources)
        val remainingResources = totalResources?.let { total -> processedResources?.let { processed -> total - processed } }
        val phaseEventCounts = calculatePhaseEventCounts(run.runId)

        var updatedRun =
            run.copy(
                currentPhase = currentPhase,
                phaseStartedAt = if (currentPhase != run.currentPhase) eventTimestamp else run.phaseStartedAt,
                lastEventTimestamp = eventTimestamp.toEpochMilli(),
                errorMessage = event.errorMessage?.toString() ?: run.errorMessage,
                totalResources = totalResources,
                processedResources = processedResources,
                remainingResources = remainingResources,
                initiatingEventsCount = phaseEventCounts["INITIATING"]?.toInt(),
                harvestingEventsCount = phaseEventCounts["HARVESTING"]?.toInt(),
                reasoningEventsCount = phaseEventCounts["REASONING"]?.toInt(),
                rdfParsingEventsCount = phaseEventCounts["RDF_PARSING"]?.toInt(),
                resourceProcessingEventsCount = phaseEventCounts["RESOURCE_PROCESSING"]?.toInt(),
                searchProcessingEventsCount = phaseEventCounts["SEARCH_PROCESSING"]?.toInt(),
                aiSearchProcessingEventsCount = phaseEventCounts["AI_SEARCH_PROCESSING"]?.toInt(),
                sparqlProcessingEventsCount = phaseEventCounts["SPARQL_PROCESSING"]?.toInt(),
                updatedAt = Instant.now(),
            )

        // Update phase durations based on phase
        when (event.phase.name) {
            "HARVESTING" -> {
                if (startTime != null && run.runStartedAt != null) {
                    updatedRun =
                        updatedRun.copy(
                            initDurationMs = ChronoUnit.MILLIS.between(run.runStartedAt, startTime),
                        )
                }
                if (startTime != null && endTime != null) {
                    updatedRun =
                        updatedRun.copy(
                            harvestDurationMs = ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "REASONING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.reasoningDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            reasoningDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "RDF_PARSING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.rdfParsingDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            rdfParsingDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "SEARCH_PROCESSING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.searchProcessingDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            searchProcessingDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "AI_SEARCH_PROCESSING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.aiSearchProcessingDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            aiSearchProcessingDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "RESOURCE_PROCESSING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.apiProcessingDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            apiProcessingDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
            "SPARQL_PROCESSING" -> {
                if (startTime != null && endTime != null) {
                    val existingDuration = run.sparqlProcessingDurationMs ?: 0L
                    updatedRun =
                        updatedRun.copy(
                            sparqlProcessingDurationMs = existingDuration + ChronoUnit.MILLIS.between(startTime, endTime),
                        )
                }
            }
        }

        // Update resource counts from extraction event (when changedResourcesCount is set)
        if (event.changedResourcesCount != null || event.unchangedResourcesCount != null || event.removedResourcesCount != null) {
            val newTotalResources =
                event.changedResourcesCount?.toInt()?.plus(event.unchangedResourcesCount?.toInt() ?: 0)?.plus(
                    event.removedResourcesCount?.toInt() ?: 0,
                )
            val newRemainingResources =
                newTotalResources?.let { total ->
                    updatedRun.processedResources?.let { processed -> total - processed }
                }
            val updatedPhaseEventCounts = calculatePhaseEventCounts(updatedRun.runId)
            updatedRun =
                updatedRun.copy(
                    totalResources = newTotalResources,
                    remainingResources = newRemainingResources,
                    initiatingEventsCount = updatedPhaseEventCounts["INITIATING"]?.toInt(),
                    harvestingEventsCount = updatedPhaseEventCounts["HARVESTING"]?.toInt(),
                    reasoningEventsCount = updatedPhaseEventCounts["REASONING"]?.toInt(),
                    rdfParsingEventsCount = updatedPhaseEventCounts["RDF_PARSING"]?.toInt(),
                    resourceProcessingEventsCount = updatedPhaseEventCounts["RESOURCE_PROCESSING"]?.toInt(),
                    searchProcessingEventsCount = updatedPhaseEventCounts["SEARCH_PROCESSING"]?.toInt(),
                    aiSearchProcessingEventsCount = updatedPhaseEventCounts["AI_SEARCH_PROCESSING"]?.toInt(),
                    sparqlProcessingEventsCount = updatedPhaseEventCounts["SPARQL_PROCESSING"]?.toInt(),
                    changedResourcesCount = event.changedResourcesCount?.toInt(),
                    unchangedResourcesCount = event.unchangedResourcesCount?.toInt(),
                    removedResourcesCount = event.removedResourcesCount?.toInt(),
                )
        }

        // Update status
        val status = determineStatus(event, run, updatedRun)
        var finalUpdatedRun = updatedRun.copy(status = status)

        // Check if harvest is complete (when all phases have endTime)
        val isComplete = checkIfAllPhasesComplete(run.runId, event)
        if (isComplete && run.runEndedAt == null) {
            // Calculate total duration as the sum of all phase durations
            val totalDuration = calculateTotalDurationFromPhases(finalUpdatedRun)

            // Calculate runEndedAt based on totalDuration to ensure consistency
            // This ensures runEndedAt - runStartedAt = totalDurationMs
            val calculatedEndTime =
                if (run.runStartedAt != null && totalDuration != null) {
                    run.runStartedAt.plusMillis(totalDuration)
                } else {
                    // Fallback to latest endTime if we can't calculate from durations
                    getLatestEndTime(run.runId) ?: endTime
                }

            finalUpdatedRun =
                finalUpdatedRun.copy(
                    runEndedAt = calculatedEndTime,
                    totalDurationMs = totalDuration,
                )
        } else if (finalUpdatedRun.status == "COMPLETED") {
            // Recalculate totalDurationMs whenever phase durations are updated for completed runs
            // This handles late-arriving events that update phase durations after completion
            val totalDuration = calculateTotalDurationFromPhases(finalUpdatedRun)
            if (totalDuration != null) {
                finalUpdatedRun = finalUpdatedRun.copy(totalDurationMs = totalDuration)
            }
        }

        // Clear errorMessage when run successfully completes (status is COMPLETED and current event has no error)
        // This handles the case where a retry fixed the issue but the old errorMessage persisted
        if (finalUpdatedRun.status == "COMPLETED" && event.errorMessage == null) {
            finalUpdatedRun = finalUpdatedRun.copy(errorMessage = null)
        }

        return finalUpdatedRun
    }

    private fun determineStatus(
        event: HarvestEvent,
        existingRun: HarvestRunEntity?,
        updatedRun: HarvestRunEntity,
    ): String {
        // If current event has an error, mark as FAILED
        if (event.errorMessage != null) {
            return "FAILED"
        }
        // Check if all phases are complete
        if (checkIfAllPhasesComplete(updatedRun.runId, event)) {
            // Mark as COMPLETED - errorMessage will be cleared later if the run completes successfully
            // This handles the case where a retry fixed the issue but the old errorMessage persisted
            return "COMPLETED"
        }
        return existingRun?.status ?: "IN_PROGRESS"
    }

    private fun checkIfAllPhasesComplete(
        runId: String,
        currentEvent: HarvestEvent,
    ): Boolean {
        // Required phases in order: HARVESTING -> REASONING -> RDF_PARSING
        // Then parallel: SEARCH_PROCESSING, AI_SEARCH_PROCESSING, RESOURCE_PROCESSING, SPARQL_PROCESSING
        val requiredPhases =
            listOf(
                "HARVESTING",
                "REASONING",
                "RDF_PARSING",
                "SEARCH_PROCESSING",
                "AI_SEARCH_PROCESSING",
                "RESOURCE_PROCESSING",
                "SPARQL_PROCESSING",
            )

        // Get the run to access resource counts
        val run = harvestRunRepository.findByRunId(runId) ?: return false
        val expectedResourceCount = (run.changedResourcesCount ?: 0) + (run.removedResourcesCount ?: 0)

        // Phases without resource identifiers (like HARVESTING) - just check that there's at least one event with endTime and no errorMessage
        val phasesWithoutResourceIds = listOf("HARVESTING")

        // Check each required phase
        return requiredPhases.all { phase ->
            if (phase in phasesWithoutResourceIds) {
                // For phases without resource identifiers, just check that there's at least one event with endTime and no errorMessage
                val count = harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(runId, phase)
                count > 0
            } else {
                // For phases with resource identifiers, count events with endTime and no errorMessage
                // Use latest events per resource to handle duplicates
                // Get all events for the phase (not just those with endTime) to find the true latest per resource
                val allPhaseEvents = harvestEventRepository.findByRunIdAndEventType(runId, phase)
                val latestEvents = getLatestEventsForPhase(allPhaseEvents)

                // Filter to only events with endTime and no errorMessage
                val completedEvents = latestEvents.filter { it.endTime != null && it.errorMessage == null }

                // Verify that the count of completed events equals changed + removed count
                // Only check if we have resource counts available
                if (expectedResourceCount > 0) {
                    completedEvents.size == expectedResourceCount
                } else {
                    // If no resource counts available yet, just check that there's at least one completed event
                    completedEvents.isNotEmpty()
                }
            }
        }
    }

    /**
     * Get the latest event for the current event's resource in the given phase.
     * The current event should already be saved, so we query for all events including it.
     * When there are duplicates, we want to use the latest event to determine state.
     */
    private fun getLatestEventForResource(
        runId: String,
        currentEvent: HarvestEvent,
        phase: String,
    ): HarvestEventEntity? {
        // Get all events for this phase and resource identifier
        // The current event should already be saved, so it will be included in the query
        val allEvents =
            when {
                currentEvent.fdkId != null -> {
                    harvestEventRepository
                        .findByRunIdAndFdkId(runId, currentEvent.fdkId.toString())
                        .filter { it.eventType == phase }
                }
                currentEvent.resourceUri != null -> {
                    harvestEventRepository
                        .findByRunIdAndResourceUri(runId, currentEvent.resourceUri.toString())
                        .filter { it.eventType == phase }
                }
                else -> {
                    // For phases without resource identifiers (like HARVESTING), get all events for this phase
                    harvestEventRepository.findByRunIdAndEventType(runId, phase)
                }
            }

        // Find the latest event by createdAt (most recent event wins)
        return allEvents.maxByOrNull { it.createdAt }
    }

    /**
     * Get the latest event for each unique resource in a phase.
     * Groups events by resource identifier (fdkId or resourceUri) and returns the latest event for each.
     * For events without resource identifiers (like HARVESTING phase), returns the single latest event.
     */
    private fun getLatestEventsForPhase(phaseEvents: List<HarvestEventEntity>): List<HarvestEventEntity> {
        if (phaseEvents.isEmpty()) {
            return emptyList()
        }

        // Check if all events have resource identifiers
        val allHaveResourceIds = phaseEvents.all { it.fdkId != null || it.resourceUri != null }

        if (!allHaveResourceIds) {
            // For phases without resource identifiers (like HARVESTING), just return the latest event overall
            val latestEvent = phaseEvents.maxByOrNull { it.createdAt }
            return if (latestEvent != null) listOf(latestEvent) else emptyList()
        }

        // Group events by resource identifier
        val eventsByResource = mutableMapOf<String, MutableList<HarvestEventEntity>>()

        phaseEvents.forEach { event ->
            val resourceKey =
                when {
                    event.fdkId != null -> "fdkId:${event.fdkId}"
                    event.resourceUri != null -> "resourceUri:${event.resourceUri}"
                    else -> "noResource:${event.id}" // Should not happen if allHaveResourceIds is true
                }
            eventsByResource.getOrPut(resourceKey) { mutableListOf() }.add(event)
        }

        // For each resource, get the latest event by createdAt
        return eventsByResource.values.mapNotNull { events ->
            events.maxByOrNull { it.createdAt }
        }
    }

    private fun getLatestEndTime(runId: String): Instant? {
        val requiredPhases =
            listOf(
                "HARVESTING",
                "REASONING",
                "RDF_PARSING",
                "SEARCH_PROCESSING",
                "AI_SEARCH_PROCESSING",
                "RESOURCE_PROCESSING",
                "SPARQL_PROCESSING",
            )

        return requiredPhases
            .flatMap { phase ->
                harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(runId, phase)
            }.mapNotNull { event ->
                event.endTime?.let { parseDateTime(it) }
            }.maxOrNull()
    }

    private fun calculateTotalResources(
        event: HarvestEvent,
        existingRun: HarvestRunEntity?,
    ): Int? {
        // Calculate total when resource counts are provided
        if (event.changedResourcesCount != null || event.unchangedResourcesCount != null || event.removedResourcesCount != null) {
            val changed = event.changedResourcesCount?.toInt() ?: 0
            val unchanged = event.unchangedResourcesCount?.toInt() ?: 0
            val removed = event.removedResourcesCount?.toInt() ?: 0
            return changed + unchanged + removed
        }
        return existingRun?.totalResources
    }

    private fun calculateProcessedResources(
        event: HarvestEvent,
        existingRun: HarvestRunEntity?,
        totalResources: Int?,
    ): Int? {
        // If total resources is not set yet, we can't calculate processed
        if (totalResources == null) {
            return existingRun?.processedResources
        }

        val currentProcessed = existingRun?.processedResources ?: 0

        // Resource processing phases - a resource is only considered processed when ALL phases are complete
        val resourceProcessingPhases =
            listOf(
                "REASONING",
                "RDF_PARSING",
                "RESOURCE_PROCESSING",
                "SEARCH_PROCESSING",
                "AI_SEARCH_PROCESSING",
                "SPARQL_PROCESSING",
            )

        // Only check for resource completion if this is one of the resource processing phases
        // Use latest event approach - only process if this is the latest event for this resource/phase
        if (event.phase.name in resourceProcessingPhases) {
            val runId = event.runId?.toString() ?: existingRun?.runId
            if (runId != null) {
                // Check if this resource has now completed all phases
                val resourceIdentifier = event.fdkId?.toString() ?: event.resourceUri?.toString()
                if (resourceIdentifier != null) {
                    // Get all events for this resource and find the latest for each phase
                    val resourceEvents =
                        when {
                            event.fdkId != null -> {
                                harvestEventRepository.findByRunIdAndFdkId(runId, event.fdkId.toString())
                            }
                            event.resourceUri != null -> {
                                harvestEventRepository.findByRunIdAndResourceUri(runId, event.resourceUri.toString())
                            }
                            else -> emptyList()
                        }

                    // Group by phase and get the latest event for each phase (by createdAt)
                    val latestEventsByPhase =
                        resourceEvents
                            .groupBy { it.eventType }
                            .mapValues { (_, events) -> events.maxByOrNull { it.createdAt } }
                            .values
                            .filterNotNull()

                    // Get unique phases that this resource has latest events for
                    val completedPhases = latestEventsByPhase.map { it.eventType }.toSet()

                    // Check if all resource processing phases are now complete for this resource
                    val allPhasesComplete = resourceProcessingPhases.all { phase -> completedPhases.contains(phase) }

                    if (allPhasesComplete) {
                        // Check if this resource was already counted as processed
                        // We do this by checking if we've already counted resources with all phases complete
                        // For simplicity, we'll count distinct resources that have all phases
                        val resourcesWithAllPhases = countResourcesWithAllPhases(runId, resourceProcessingPhases)
                        return minOf(totalResources, resourcesWithAllPhases)
                    }
                }
            }
        }

        // For other phases or if not all phases complete, keep the current processed count
        return currentProcessed
    }

    private fun calculatePhaseEventCounts(runId: String): Map<String, Long> {
        val phases =
            listOf(
                "INITIATING",
                "HARVESTING",
                "REASONING",
                "RDF_PARSING",
                "RESOURCE_PROCESSING",
                "SEARCH_PROCESSING",
                "AI_SEARCH_PROCESSING",
                "SPARQL_PROCESSING",
            )

        return phases.associateWith { phase ->
            harvestEventRepository.countByRunIdAndEventType(runId, phase)
        }
    }

    private fun countResourcesWithAllPhases(
        runId: String,
        requiredPhases: List<String>,
    ): Int {
        // Get all events for this run that are in the resource processing phases
        val allResourceEvents =
            requiredPhases.flatMap { phase ->
                harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(runId, phase)
            }

        // Group by resource identifier and get the latest event for each resource/phase combination
        // This ensures we use the latest event when there are duplicates
        val latestEventsByResourceAndPhase =
            allResourceEvents
                .groupBy { event ->
                    when {
                        event.fdkId != null -> "fdkId:${event.fdkId}"
                        event.resourceUri != null -> "resourceUri:${event.resourceUri}"
                        else -> "noResource:${event.id}"
                    }
                }.mapValues { (_, events) ->
                    events
                        .groupBy { it.eventType }
                        .mapValues { (_, phaseEvents) -> phaseEvents.maxByOrNull { it.createdAt } }
                        .values
                        .filterNotNull()
                }

        // Count resources that have latest events for all required phases
        val resourcesWithAllPhases =
            latestEventsByResourceAndPhase.values.count { latestEvents ->
                val phases = latestEvents.map { it.eventType }.toSet()
                phases.containsAll(requiredPhases)
            }

        return resourcesWithAllPhases
    }

    @Scheduled(fixedDelayString = "\${app.harvest.stale-check-interval-ms:300000}", initialDelay = 60000)
    @Transactional
    fun markStaleRunsAsFailed() {
        try {
            val staleBefore = Instant.now().minus(staleTimeoutMinutes, ChronoUnit.MINUTES)
            val staleRuns = harvestRunRepository.findStaleRuns(staleBefore)

            if (staleRuns.isNotEmpty()) {
                logger.warn("Found ${staleRuns.size} stale harvest run(s) that haven't been updated in $staleTimeoutMinutes minutes")
                staleRuns.forEach { run ->
                    val updatedRun =
                        run.copy(
                            status = "FAILED",
                            errorMessage = "Harvest run timed out - no events received for $staleTimeoutMinutes minutes",
                            runEndedAt = run.updatedAt,
                            updatedAt = Instant.now(),
                        )
                    harvestRunRepository.save(updatedRun)
                    logger.info("Marked stale harvest run ${run.runId} as FAILED (last updated: ${run.updatedAt})")
                    // Record metrics for failed run
                    harvestMetricsService.recordRunCompleted(updatedRun)
                }
            }
        } catch (e: Exception) {
            logger.error("Error marking stale runs as failed", e)
        }
    }

    private fun parseDateTime(dateString: String): Instant? =
        try {
            // Try parsing as ISO format first
            Instant.parse(dateString)
        } catch (e: Exception) {
            try {
                // Try parsing as "yyyy-MM-dd HH:mm:ss Z" format (e.g., "2025-12-11 13:21:38 +0100")
                java.time.format.DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss Z")
                    .parse(dateString)
                    .let { temporalAccessor ->
                        java.time.OffsetDateTime
                            .from(temporalAccessor)
                            .toInstant()
                    }
            } catch (e2: Exception) {
                try {
                    // Try parsing as "yyyy-MM-dd HH:mm:ss" format (without timezone)
                    java.time.LocalDateTime
                        .parse(
                            dateString,
                            java.time.format.DateTimeFormatter
                                .ofPattern("yyyy-MM-dd HH:mm:ss"),
                        ).atZone(java.time.ZoneId.systemDefault())
                        .toInstant()
                } catch (e3: Exception) {
                    logger.warn("Could not parse date string: $dateString", e3)
                    null
                }
            }
        }

    // Methods for current state and performance metrics

    fun getCurrentState(
        dataSourceId: String,
        dataType: String? = null,
    ): Pair<List<HarvestCurrentState>, HttpStatus> =
        try {
            val runs =
                if (dataType != null) {
                    harvestRunRepository.findCurrentRun(dataSourceId, dataType)?.let { listOf(it) } ?: emptyList()
                } else {
                    harvestRunRepository.findCurrentRunsByDataSourceId(dataSourceId)
                }

            val states =
                runs.map { run ->
                    HarvestCurrentState(
                        dataSourceId = run.dataSourceId,
                        dataType = run.dataType,
                        currentPhase = run.currentPhase,
                        phaseStartedAt = run.phaseStartedAt,
                        lastEventTimestamp = run.lastEventTimestamp,
                        errorMessage = run.errorMessage,
                        totalResources = run.totalResources,
                        processedResources = run.processedResources,
                        remainingResources = run.remainingResources,
                        phaseEventCounts =
                            no.fdk.harvestadmin.model.PhaseEventCounts(
                                initiatingEventsCount = run.initiatingEventsCount,
                                harvestingEventsCount = run.harvestingEventsCount,
                                reasoningEventsCount = run.reasoningEventsCount,
                                rdfParsingEventsCount = run.rdfParsingEventsCount,
                                resourceProcessingEventsCount = run.resourceProcessingEventsCount,
                                searchProcessingEventsCount = run.searchProcessingEventsCount,
                                aiSearchProcessingEventsCount = run.aiSearchProcessingEventsCount,
                                sparqlProcessingEventsCount = run.sparqlProcessingEventsCount,
                            ),
                        changedResourcesCount = run.changedResourcesCount,
                        unchangedResourcesCount = run.unchangedResourcesCount,
                        removedResourcesCount = run.removedResourcesCount,
                        status = run.status,
                        createdAt = run.createdAt,
                        updatedAt = run.updatedAt,
                    )
                }

            Pair(states, HttpStatus.OK)
        } catch (e: Exception) {
            logger.error("Error getting current state for dataSourceId: $dataSourceId, dataType: $dataType", e)
            Pair(emptyList(), HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getAllInProgressStates(): List<HarvestRunDetails> =
        try {
            harvestRunRepository.findAllInProgress().map { run ->
                HarvestRunDetails(
                    runId = run.runId,
                    dataSourceId = run.dataSourceId,
                    dataType = run.dataType,
                    runStartedAt = run.runStartedAt,
                    runEndedAt = run.runEndedAt,
                    totalDurationMs = run.totalDurationMs,
                    phaseDurations =
                        PhaseDurations(
                            initDurationMs = run.initDurationMs,
                            harvestDurationMs = run.harvestDurationMs,
                            reasoningDurationMs = run.reasoningDurationMs,
                            rdfParsingDurationMs = run.rdfParsingDurationMs,
                            searchProcessingDurationMs = run.searchProcessingDurationMs,
                            aiSearchProcessingDurationMs = run.aiSearchProcessingDurationMs,
                            apiProcessingDurationMs = run.apiProcessingDurationMs,
                            sparqlProcessingDurationMs = run.sparqlProcessingDurationMs,
                        ),
                    resourceCounts =
                        ResourceCounts(
                            totalResources = run.totalResources,
                            changedResourcesCount = run.changedResourcesCount,
                            unchangedResourcesCount = run.unchangedResourcesCount,
                            removedResourcesCount = run.removedResourcesCount,
                            phaseEventCounts =
                                no.fdk.harvestadmin.model.PhaseEventCounts(
                                    initiatingEventsCount = run.initiatingEventsCount,
                                    harvestingEventsCount = run.harvestingEventsCount,
                                    reasoningEventsCount = run.reasoningEventsCount,
                                    rdfParsingEventsCount = run.rdfParsingEventsCount,
                                    resourceProcessingEventsCount = run.resourceProcessingEventsCount,
                                    searchProcessingEventsCount = run.searchProcessingEventsCount,
                                    aiSearchProcessingEventsCount = run.aiSearchProcessingEventsCount,
                                    sparqlProcessingEventsCount = run.sparqlProcessingEventsCount,
                                ),
                        ),
                    status = run.status,
                    errorMessage = run.errorMessage,
                    createdAt = run.createdAt,
                    updatedAt = run.updatedAt,
                )
            }
        } catch (e: Exception) {
            logger.error("Error getting all in-progress states", e)
            emptyList()
        }

    fun getPerformanceMetrics(
        dataSourceId: String,
        dataType: String,
        daysBack: Int? = null,
        startDate: Instant? = null,
        endDate: Instant? = null,
        limit: Int? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> =
        try {
            val runs =
                when {
                    limit != null -> {
                        // Get last N completed runs
                        harvestRunRepository.findLastCompletedRuns(dataSourceId, dataType, PageRequest.of(0, limit))
                    }
                    startDate != null && endDate != null -> {
                        // Get runs from date range
                        harvestRunRepository.findCompletedRunsByDateRange(dataSourceId, dataType, startDate, endDate)
                    }
                    startDate != null -> {
                        // Get runs from startDate to now
                        harvestRunRepository.findCompletedRunsByDateRange(dataSourceId, dataType, startDate, Instant.now())
                    }
                    else -> {
                        // Default: use daysBack (backward compatibility)
                        val start = Instant.now().minus((daysBack ?: 30).toLong(), ChronoUnit.DAYS)
                        harvestRunRepository.findCompletedRuns(dataSourceId, dataType, start)
                    }
                }

            if (runs.isEmpty()) {
                Pair(null, HttpStatus.NOT_FOUND)
            } else {
                // Filter out runs with errors - they can have misleadingly low durations
                val successfulRuns = runs.filter { it.status == "COMPLETED" && it.errorMessage == null }
                val completedRuns = runs.filter { it.status == "COMPLETED" }
                val failedRuns = runs.filter { it.status == "FAILED" }

                val periodStart = runs.minOfOrNull { it.runStartedAt } ?: Instant.now()
                val periodEnd = runs.maxOfOrNull { it.runStartedAt } ?: Instant.now()

                val metrics =
                    HarvestPerformanceMetrics(
                        dataSourceId = dataSourceId,
                        dataType = dataType,
                        totalRuns = runs.size,
                        completedRuns = completedRuns.size,
                        failedRuns = failedRuns.size,
                        averageTotalDurationMs = calculateAverage(successfulRuns) { it.totalDurationMs?.toDouble() },
                        averageHarvestDurationMs = calculateAverage(successfulRuns) { it.harvestDurationMs?.toDouble() },
                        averageReasoningDurationMs = calculateAverage(successfulRuns) { it.reasoningDurationMs?.toDouble() },
                        averageRdfParsingDurationMs = calculateAverage(successfulRuns) { it.rdfParsingDurationMs?.toDouble() },
                        averageSearchProcessingDurationMs = calculateAverage(successfulRuns) { it.searchProcessingDurationMs?.toDouble() },
                        averageAiSearchProcessingDurationMs =
                            calculateAverage(
                                successfulRuns,
                            ) { it.aiSearchProcessingDurationMs?.toDouble() },
                        averageApiProcessingDurationMs = calculateAverage(successfulRuns) { it.apiProcessingDurationMs?.toDouble() },
                        averageSparqlProcessingDurationMs = calculateAverage(successfulRuns) { it.sparqlProcessingDurationMs?.toDouble() },
                        periodStart = periodStart,
                        periodEnd = periodEnd,
                    )

                Pair(metrics, HttpStatus.OK)
            }
        } catch (e: Exception) {
            logger.error("Error getting performance metrics for dataSourceId: $dataSourceId, dataType: $dataType", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getAllPerformanceMetrics(
        daysBack: Int? = null,
        startDate: Instant? = null,
        endDate: Instant? = null,
        limit: Int? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> =
        try {
            val runs =
                when {
                    limit != null -> {
                        // Get last N completed runs across all data sources
                        harvestRunRepository.findLastAllCompletedRuns(PageRequest.of(0, limit))
                    }
                    startDate != null && endDate != null -> {
                        // Get runs from date range across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, endDate)
                    }
                    startDate != null -> {
                        // Get runs from startDate to now across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, Instant.now())
                    }
                    else -> {
                        // Default: use daysBack (backward compatibility)
                        val start = Instant.now().minus((daysBack ?: 30).toLong(), ChronoUnit.DAYS)
                        harvestRunRepository.findAllCompletedRuns(start)
                    }
                }

            if (runs.isEmpty()) {
                Pair(null, HttpStatus.NOT_FOUND)
            } else {
                // Filter out runs with errors - they can have misleadingly low durations
                val successfulRuns = runs.filter { it.status == "COMPLETED" && it.errorMessage == null }
                val completedRuns = runs.filter { it.status == "COMPLETED" }
                val failedRuns = runs.filter { it.status == "FAILED" }

                val periodStart = runs.minOfOrNull { it.runStartedAt } ?: Instant.now()
                val periodEnd = runs.maxOfOrNull { it.runStartedAt } ?: Instant.now()

                val metrics =
                    HarvestPerformanceMetrics(
                        dataSourceId = null, // Global metrics
                        dataType = null, // Global metrics
                        totalRuns = runs.size,
                        completedRuns = completedRuns.size,
                        failedRuns = failedRuns.size,
                        averageTotalDurationMs = calculateAverage(successfulRuns) { it.totalDurationMs?.toDouble() },
                        averageHarvestDurationMs = calculateAverage(successfulRuns) { it.harvestDurationMs?.toDouble() },
                        averageReasoningDurationMs = calculateAverage(successfulRuns) { it.reasoningDurationMs?.toDouble() },
                        averageRdfParsingDurationMs = calculateAverage(successfulRuns) { it.rdfParsingDurationMs?.toDouble() },
                        averageSearchProcessingDurationMs = calculateAverage(successfulRuns) { it.searchProcessingDurationMs?.toDouble() },
                        averageAiSearchProcessingDurationMs =
                            calculateAverage(
                                successfulRuns,
                            ) { it.aiSearchProcessingDurationMs?.toDouble() },
                        averageApiProcessingDurationMs = calculateAverage(successfulRuns) { it.apiProcessingDurationMs?.toDouble() },
                        averageSparqlProcessingDurationMs = calculateAverage(successfulRuns) { it.sparqlProcessingDurationMs?.toDouble() },
                        periodStart = periodStart,
                        periodEnd = periodEnd,
                    )

                Pair(metrics, HttpStatus.OK)
            }
        } catch (e: Exception) {
            logger.error("Error getting all performance metrics", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getHarvestRun(runId: String): Pair<HarvestRunDetails?, HttpStatus> =
        try {
            val run = harvestRunRepository.findByRunId(runId)
            if (run == null) {
                Pair(null, HttpStatus.NOT_FOUND)
            } else {
                Pair(
                    HarvestRunDetails(
                        runId = run.runId,
                        dataSourceId = run.dataSourceId,
                        dataType = run.dataType,
                        runStartedAt = run.runStartedAt,
                        runEndedAt = run.runEndedAt,
                        totalDurationMs = run.totalDurationMs,
                        phaseDurations =
                            PhaseDurations(
                                initDurationMs = run.initDurationMs,
                                harvestDurationMs = run.harvestDurationMs,
                                reasoningDurationMs = run.reasoningDurationMs,
                                rdfParsingDurationMs = run.rdfParsingDurationMs,
                                searchProcessingDurationMs = run.searchProcessingDurationMs,
                                aiSearchProcessingDurationMs = run.aiSearchProcessingDurationMs,
                                apiProcessingDurationMs = run.apiProcessingDurationMs,
                                sparqlProcessingDurationMs = run.sparqlProcessingDurationMs,
                            ),
                        resourceCounts =
                            ResourceCounts(
                                totalResources = run.totalResources,
                                changedResourcesCount = run.changedResourcesCount,
                                unchangedResourcesCount = run.unchangedResourcesCount,
                                removedResourcesCount = run.removedResourcesCount,
                                phaseEventCounts =
                                    no.fdk.harvestadmin.model.PhaseEventCounts(
                                        initiatingEventsCount = run.initiatingEventsCount,
                                        harvestingEventsCount = run.harvestingEventsCount,
                                        reasoningEventsCount = run.reasoningEventsCount,
                                        rdfParsingEventsCount = run.rdfParsingEventsCount,
                                        resourceProcessingEventsCount = run.resourceProcessingEventsCount,
                                        searchProcessingEventsCount = run.searchProcessingEventsCount,
                                        aiSearchProcessingEventsCount = run.aiSearchProcessingEventsCount,
                                        sparqlProcessingEventsCount = run.sparqlProcessingEventsCount,
                                    ),
                            ),
                        status = run.status,
                        errorMessage = run.errorMessage,
                        createdAt = run.createdAt,
                        updatedAt = run.updatedAt,
                    ),
                    HttpStatus.OK,
                )
            }
        } catch (e: Exception) {
            logger.error("Error getting harvest run for runId: $runId", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getHarvestRuns(
        dataSourceId: String? = null,
        dataType: String? = null,
        status: String? = null,
        offset: Int = 0,
        limit: Int = 50,
    ): Pair<List<HarvestRunDetails>, Long> =
        try {
            // Calculate page number from offset (Spring Data uses 0-indexed page numbers)
            val page = if (limit > 0) offset / limit else 0
            val pageable = PageRequest.of(page, limit)
            val runs = harvestRunRepository.findRunsWithFilters(dataSourceId, dataType, status, pageable)
            val totalCount = harvestRunRepository.countRunsWithFilters(dataSourceId, dataType, status)

            val runDetails =
                runs.map { run ->
                    HarvestRunDetails(
                        runId = run.runId,
                        dataSourceId = run.dataSourceId,
                        dataType = run.dataType,
                        runStartedAt = run.runStartedAt,
                        runEndedAt = run.runEndedAt,
                        totalDurationMs = run.totalDurationMs,
                        phaseDurations =
                            PhaseDurations(
                                initDurationMs = run.initDurationMs,
                                harvestDurationMs = run.harvestDurationMs,
                                reasoningDurationMs = run.reasoningDurationMs,
                                rdfParsingDurationMs = run.rdfParsingDurationMs,
                                searchProcessingDurationMs = run.searchProcessingDurationMs,
                                aiSearchProcessingDurationMs = run.aiSearchProcessingDurationMs,
                                apiProcessingDurationMs = run.apiProcessingDurationMs,
                                sparqlProcessingDurationMs = run.sparqlProcessingDurationMs,
                            ),
                        resourceCounts =
                            ResourceCounts(
                                totalResources = run.totalResources,
                                changedResourcesCount = run.changedResourcesCount,
                                unchangedResourcesCount = run.unchangedResourcesCount,
                                removedResourcesCount = run.removedResourcesCount,
                                phaseEventCounts =
                                    no.fdk.harvestadmin.model.PhaseEventCounts(
                                        initiatingEventsCount = run.initiatingEventsCount,
                                        harvestingEventsCount = run.harvestingEventsCount,
                                        reasoningEventsCount = run.reasoningEventsCount,
                                        rdfParsingEventsCount = run.rdfParsingEventsCount,
                                        resourceProcessingEventsCount = run.resourceProcessingEventsCount,
                                        searchProcessingEventsCount = run.searchProcessingEventsCount,
                                        aiSearchProcessingEventsCount = run.aiSearchProcessingEventsCount,
                                        sparqlProcessingEventsCount = run.sparqlProcessingEventsCount,
                                    ),
                            ),
                        status = run.status,
                        errorMessage = run.errorMessage,
                        createdAt = run.createdAt,
                        updatedAt = run.updatedAt,
                    )
                }
            Pair(runDetails, totalCount)
        } catch (e: Exception) {
            logger.error("Error getting harvest runs for dataSourceId: $dataSourceId, dataType: $dataType, status: $status", e)
            Pair(emptyList(), 0L)
        }

    private fun <T> calculateAverage(
        items: List<T>,
        extractor: (T) -> Double?,
    ): Double? {
        val values = items.mapNotNull(extractor)
        return if (values.isNotEmpty()) {
            values.average()
        } else {
            null
        }
    }

    /**
     * Calculate total duration as the sum of all phase durations.
     * This includes: INITIATING, HARVESTING, REASONING, RDF_PARSING,
     * SEARCH_PROCESSING, AI_SEARCH_PROCESSING, RESOURCE_PROCESSING, SPARQL_PROCESSING
     */
    private fun calculateTotalDurationFromPhases(run: HarvestRunEntity): Long? {
        val initDuration = run.initDurationMs ?: 0L
        val harvestDuration = run.harvestDurationMs ?: 0L
        val reasoningDuration = run.reasoningDurationMs ?: 0L
        val rdfParsingDuration = run.rdfParsingDurationMs ?: 0L
        val searchProcessingDuration = run.searchProcessingDurationMs ?: 0L
        val aiSearchProcessingDuration = run.aiSearchProcessingDurationMs ?: 0L
        val resourceProcessingDuration = run.apiProcessingDurationMs ?: 0L
        val sparqlProcessingDuration = run.sparqlProcessingDurationMs ?: 0L

        val total =
            initDuration +
                harvestDuration +
                reasoningDuration +
                rdfParsingDuration +
                searchProcessingDuration +
                aiSearchProcessingDuration +
                resourceProcessingDuration +
                sparqlProcessingDuration

        return if (total > 0) total else null
    }
}
