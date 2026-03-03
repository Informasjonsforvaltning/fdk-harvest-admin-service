package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.model.HarvestCurrentState
import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.model.PhaseCompletion
import no.fdk.harvestadmin.model.PhaseDurations
import no.fdk.harvestadmin.model.ResourceCounts
import no.fdk.harvestadmin.model.RunCompletionStatus
import no.fdk.harvestadmin.repository.DataSourceRepository
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
    private val dataSourceRepository: DataSourceRepository,
    private val harvestMetricsService: HarvestMetricsService,
    @param:Value("\${app.harvest.stale-timeout-minutes:30}") private val staleTimeoutMinutes: Long,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /** Resolves allowed publisher IDs (orgs) to data source IDs for run filtering. Returns null when no restriction (e.g. system admin / API key). */
    private fun resolveAllowedDataSourceIds(allowedPublisherIds: List<String>?): List<String>? {
        if (allowedPublisherIds == null) return null
        if (allowedPublisherIds.isEmpty()) return emptyList()
        return dataSourceRepository.findByPublisherIdIn(allowedPublisherIds).map { it.id }
    }

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

            // Record resource counts during run (including 0) so Grafana "Resources per Run" gets data
            harvestMetricsService.recordRunResourceCounts(savedRun)
        }

        // Record metrics if status changed (always record completion/failure metrics)
        if (oldStatus != savedRun.status) {
            harvestMetricsService.recordRunCompleted(savedRun)
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

        // For INITIATING, capture removeAll and forced from the event
        val removeAll = if (event.phase.name == "INITIATING") event.removeAll else run.removeAll
        val forced = if (event.phase.name == "INITIATING") event.forced else run.forced

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
                removeAll = removeAll,
                forced = forced,
                updatedAt = Instant.now(),
            )

        // Update phase durations based on phase
        when (event.phase.name) {
            "HARVESTING" -> {
                if (startTime != null) {
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
        if (event.changedResourcesCount != null || event.removedResourcesCount != null) {
            val newTotalResources =
                (event.changedResourcesCount?.toInt() ?: 0).plus(
                    event.removedResourcesCount?.toInt() ?: 0,
                )
            val newRemainingResources =
                updatedRun.processedResources?.let { processed -> newTotalResources - processed }
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
                    removedResourcesCount = event.removedResourcesCount?.toInt(),
                )
        }

        // Evaluate completion across phases using the latest in-memory run state
        val completionStatus = checkIfAllPhasesComplete(updatedRun)

        // Determine status based on error and completion state
        val status =
            if (event.errorMessage != null) {
                "FAILED"
            } else if (completionStatus.allPhasesComplete) {
                "COMPLETED"
            } else {
                existingStatusOrDefault(run)
            }

        var finalUpdatedRun = updatedRun.copy(status = status)

        // Check if harvest is complete (when all required phases are complete)
        val isComplete = completionStatus.allPhasesComplete

        // When the run is still in progress, record per-phase resource shortfall metrics
        if (!isComplete) {
            completionStatus.phases
                .filter { it.required && !it.complete && it.expectedResources != null }
                .forEach { phase ->
                    val expected = phase.expectedResources!!
                    val shortfall = expected - phase.completedResources
                    if (shortfall > 0) {
                        harvestMetricsService.recordPhaseResourceShortfall(updatedRun.dataType, phase.phase, shortfall)
                    }
                }
        }
        if (isComplete && run.runEndedAt == null) {
            // Calculate total duration as the sum of all phase durations
            val totalDuration = calculateTotalDurationFromPhases(finalUpdatedRun)

            // Calculate runEndedAt based on totalDuration to ensure consistency
            // This ensures runEndedAt - runStartedAt = totalDurationMs
            val calculatedEndTime =
                if (totalDuration != null) {
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

    private fun existingStatusOrDefault(run: HarvestRunEntity): String = run.status.ifBlank { "IN_PROGRESS" }

    /**
     * Evaluate whether all required phases are complete for a harvest run and
     * return detailed per-phase completion information.
     *
     * Rules:
     * - For phases without per-resource identifiers (e.g. HARVESTING), we only
     *   require at least one successful event (endTime != null, no error).
     * - For resource-based phases, when an expected resource count is known
     *   (changed + removed > 0), we require that the number of completed
     *   resources is **at least** the expected count. More is allowed
     *   (retries/duplicates), fewer will block completion.
     * - Optional phases (AI_SEARCH_PROCESSING, SPARQL_PROCESSING) do not block
     *   completion when there are no events at all for that phase in the run.
     *   If events do exist, they behave like required phases.
     */
    private fun checkIfAllPhasesComplete(run: HarvestRunEntity): RunCompletionStatus {
        val expectedResourceCount = (run.changedResourcesCount ?: 0) + (run.removedResourcesCount ?: 0)
        val hasExplicitResourceCounts =
            run.changedResourcesCount != null ||
                run.removedResourcesCount != null ||
                run.totalResources != null

        // Phases without resource identifiers (like HARVESTING) - just check that there's at least one event
        val phasesWithoutResourceIds = listOf(HarvestPhaseConfig.HARVESTING_PHASE)

        val phaseCompletions = mutableListOf<PhaseCompletion>()
        var allRequiredComplete = true

        // Special case: we explicitly know there are zero resources to process (0 changed, 0 removed).
        // In this case, a successful HARVESTING phase is enough to consider the run completed, and
        // all resource-processing phases are treated as optional/complete.
        if (hasExplicitResourceCounts && expectedResourceCount == 0) {
            HarvestPhaseConfig.allPhasesInCompletionOrder.forEach { phase ->
                if (phase in phasesWithoutResourceIds) {
                    val count =
                        harvestEventRepository
                            .countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(run.runId, phase)
                    val hasCompletedEvent = count > 0
                    val complete = hasCompletedEvent
                    if (!complete) {
                        allRequiredComplete = false
                    }
                    phaseCompletions.add(
                        PhaseCompletion(
                            phase = phase,
                            required = true,
                            expectedResources = null,
                            completedResources = if (hasCompletedEvent) 1 else 0,
                            complete = complete,
                        ),
                    )
                } else {
                    // No resources to process for resource-based phases in this scenario.
                    phaseCompletions.add(
                        PhaseCompletion(
                            phase = phase,
                            required = false,
                            expectedResources = null,
                            completedResources = 0,
                            complete = true,
                        ),
                    )
                }
            }

            if (!allRequiredComplete) {
                val blockingPhases =
                    phaseCompletions
                        .filter { it.required && !it.complete }
                        .joinToString { "${it.phase}(expected=${it.expectedResources ?: "?"}, completed=${it.completedResources})" }
                logger.debug(
                    "Run ${run.runId} not yet COMPLETED. Blocking phases: $blockingPhases",
                )
            }

            return RunCompletionStatus(
                allPhasesComplete = allRequiredComplete,
                phases = phaseCompletions,
            )
        }

        HarvestPhaseConfig.allPhasesInCompletionOrder.forEach { phase ->
            val isOptionalByConfig = phase in HarvestPhaseConfig.optionalPhases

            if (phase in phasesWithoutResourceIds) {
                val count =
                    harvestEventRepository
                        .countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(run.runId, phase)
                val hasCompletedEvent = count > 0
                // HARVESTING is always required
                val complete = hasCompletedEvent
                if (!complete) {
                    allRequiredComplete = false
                }
                phaseCompletions.add(
                    PhaseCompletion(
                        phase = phase,
                        required = true,
                        expectedResources = null,
                        completedResources = if (hasCompletedEvent) 1 else 0,
                        complete = complete,
                    ),
                )
            } else {
                // For phases with resource identifiers, operate on the latest event per resource
                val allPhaseEvents = harvestEventRepository.findByRunIdAndEventType(run.runId, phase)
                val latestEvents = getLatestEventsForPhase(allPhaseEvents)
                val completedEvents = latestEvents.filter { it.endTime != null && it.errorMessage == null }
                val completedResources = completedEvents.size
                val hasAnyCompleted = completedResources > 0

                // Determine whether this phase is effectively required for this run.
                // Optional phases with no events at all do not block completion and are
                // treated as complete with no expected count.
                val isOptionalAndUnused = isOptionalByConfig && allPhaseEvents.isEmpty()
                val required = !isOptionalAndUnused

                val expected =
                    if (!isOptionalAndUnused && expectedResourceCount > 0) {
                        expectedResourceCount
                    } else {
                        null
                    }

                val complete =
                    when {
                        isOptionalAndUnused -> true
                        expected != null -> completedResources >= expected
                        else -> hasAnyCompleted
                    }

                if (required && !complete) {
                    allRequiredComplete = false
                }

                if (expected != null && completedResources > expected) {
                    logger.debug(
                        "Run ${run.runId} phase $phase has more completed resources ($completedResources) than expected ($expected).",
                    )
                }

                phaseCompletions.add(
                    PhaseCompletion(
                        phase = phase,
                        required = required,
                        expectedResources = expected,
                        completedResources = completedResources,
                        complete = complete,
                    ),
                )
            }
        }

        if (!allRequiredComplete) {
            val blockingPhases =
                phaseCompletions
                    .filter { it.required && !it.complete }
                    .joinToString { "${it.phase}(expected=${it.expectedResources ?: "?"}, completed=${it.completedResources})" }
            logger.debug(
                "Run ${run.runId} not yet COMPLETED. Blocking phases: $blockingPhases",
            )
        }

        return RunCompletionStatus(
            allPhasesComplete = allRequiredComplete,
            phases = phaseCompletions,
        )
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
        if (event.changedResourcesCount != null || event.removedResourcesCount != null) {
            val changed = event.changedResourcesCount?.toInt() ?: 0
            val removed = event.removedResourcesCount?.toInt() ?: 0
            return changed + removed
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

        val resourceProcessingPhases = HarvestPhaseConfig.resourceProcessingPhases
        val runId = event.runId?.toString() ?: existingRun?.runId

        // Only recompute processed resources when we are in a resource-processing phase and have a runId
        if (runId == null || event.phase.name !in resourceProcessingPhases) {
            return existingRun?.processedResources
        }

        // Use the same effective phase logic as completion: optional phases without events
        // are not required when counting fully processed resources for the run.
        val resourcesWithAllPhases = countResourcesWithAllPhases(runId, resourceProcessingPhases)
        return minOf(totalResources, resourcesWithAllPhases)
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
        // Optional phases without any events in this run should not be treated as required when
        // counting fully processed resources.
        val effectiveRequiredPhases =
            requiredPhases.filter { phase ->
                if (phase in HarvestPhaseConfig.optionalPhases) {
                    val eventCount = harvestEventRepository.countByRunIdAndEventType(runId, phase)
                    eventCount > 0
                } else {
                    true
                }
            }

        // Get all events for this run that are in the effective resource-processing phases
        val allResourceEvents =
            effectiveRequiredPhases.flatMap { phase ->
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
                phases.containsAll(effectiveRequiredPhases)
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

    fun getCurrentState(dataSourceId: String): Pair<List<HarvestCurrentState>, HttpStatus> =
        try {
            val run = harvestRunRepository.findFirstByDataSourceIdOrderByRunStartedAtDesc(dataSourceId)
            val runs = run?.let { listOf(it) } ?: emptyList()

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
                        removedResourcesCount = run.removedResourcesCount,
                        removeAll = run.removeAll,
                        forced = run.forced,
                        status = run.status,
                        createdAt = run.createdAt,
                        updatedAt = run.updatedAt,
                    )
                }

            Pair(states, HttpStatus.OK)
        } catch (e: Exception) {
            logger.error("Error getting current state for dataSourceId: $dataSourceId", e)
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
                    removeAll = run.removeAll,
                    forced = run.forced,
                    status = run.status,
                    errorMessage = run.errorMessage,
                    createdAt = run.createdAt,
                    updatedAt = run.updatedAt,
                    completionStatus = checkIfAllPhasesComplete(run),
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
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> {
        return try {
            if (allowedPublisherIds != null) {
                val dataSource = dataSourceRepository.findById(dataSourceId).orElse(null)
                if (dataSource == null || dataSource.publisherId !in allowedPublisherIds) {
                    return Pair(null, HttpStatus.FORBIDDEN)
                }
            }
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
    }

    fun getAllPerformanceMetrics(
        daysBack: Int? = null,
        startDate: Instant? = null,
        endDate: Instant? = null,
        limit: Int? = null,
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> {
        return try {
            val allowedDataSourceIds = resolveAllowedDataSourceIds(allowedPublisherIds)
            if (allowedPublisherIds != null && allowedDataSourceIds != null && allowedDataSourceIds.isEmpty()) {
                return Pair(null, HttpStatus.NOT_FOUND)
            }
            val runs =
                when {
                    limit != null -> {
                        // Get last N completed runs across all data sources
                        harvestRunRepository.findLastAllCompletedRuns(allowedDataSourceIds, PageRequest.of(0, limit))
                    }
                    startDate != null && endDate != null -> {
                        // Get runs from date range across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, endDate, allowedDataSourceIds)
                    }
                    startDate != null -> {
                        // Get runs from startDate to now across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, Instant.now(), allowedDataSourceIds)
                    }
                    else -> {
                        // Default: use daysBack (backward compatibility)
                        val start = Instant.now().minus((daysBack ?: 30).toLong(), ChronoUnit.DAYS)
                        harvestRunRepository.findAllCompletedRuns(start, allowedDataSourceIds)
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
    }

    fun getHarvestRun(
        runId: String,
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestRunDetails?, HttpStatus> {
        return try {
            val run = harvestRunRepository.findByRunId(runId) ?: return Pair(null, HttpStatus.NOT_FOUND)
            if (allowedPublisherIds != null) {
                val dataSource = dataSourceRepository.findById(run.dataSourceId).orElse(null)
                if (dataSource == null || dataSource.publisherId !in allowedPublisherIds) {
                    return Pair(null, HttpStatus.FORBIDDEN)
                }
            }
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
                    removeAll = run.removeAll,
                    forced = run.forced,
                    status = run.status,
                    errorMessage = run.errorMessage,
                    createdAt = run.createdAt,
                    updatedAt = run.updatedAt,
                    completionStatus = checkIfAllPhasesComplete(run),
                ),
                HttpStatus.OK,
            )
        } catch (e: Exception) {
            logger.error("Error getting harvest run for runId: $runId", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    fun getHarvestRuns(
        dataSourceId: String? = null,
        dataType: String? = null,
        status: String? = null,
        offset: Int = 0,
        limit: Int = 50,
        allowedPublisherIds: List<String>? = null,
    ): Pair<List<HarvestRunDetails>, Long> {
        return try {
            val allowedDataSourceIds = resolveAllowedDataSourceIds(allowedPublisherIds)
            if (allowedPublisherIds != null && allowedDataSourceIds != null && allowedDataSourceIds.isEmpty()) {
                return Pair(emptyList(), 0L)
            }
            // Calculate page number from offset (Spring Data uses 0-indexed page numbers)
            val page = if (limit > 0) offset / limit else 0
            val pageable = PageRequest.of(page, limit)
            val runs = harvestRunRepository.findRunsWithFilters(dataSourceId, dataType, status, allowedDataSourceIds, pageable)
            val totalCount = harvestRunRepository.countRunsWithFilters(dataSourceId, dataType, status, allowedDataSourceIds)

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
                        removeAll = run.removeAll,
                        forced = run.forced,
                        status = run.status,
                        errorMessage = run.errorMessage,
                        createdAt = run.createdAt,
                        updatedAt = run.updatedAt,
                        completionStatus = if (run.status == "COMPLETED") null else checkIfAllPhasesComplete(run),
                    )
                }
            Pair(runDetails, totalCount)
        } catch (e: Exception) {
            logger.error("Error getting harvest runs for dataSourceId: $dataSourceId, dataType: $dataType, status: $status", e)
            Pair(emptyList(), 0L)
        }
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
