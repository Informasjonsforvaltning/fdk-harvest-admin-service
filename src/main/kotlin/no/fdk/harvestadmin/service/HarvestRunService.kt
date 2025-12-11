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
import org.springframework.data.domain.PageRequest
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.temporal.ChronoUnit

@Service
class HarvestRunService(
    private val harvestEventRepository: HarvestEventRepository,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun persistEvent(event: HarvestEvent) {
        try {
            // Get run to use runStartedAt as fallback for timestamp
            val runId = event.runId?.toString()
            val dataSourceId = event.dataSourceId.toString()
            val dataType = event.dataType.name
            val currentRun =
                runId?.let { harvestRunRepository.findByRunId(it) }
                    ?: harvestRunRepository.findCurrentRun(dataSourceId, dataType)

            // Use the runId from the found run if event doesn't have one
            val eventRunId = runId ?: currentRun?.runId

            // Use startTime if available, otherwise fallback to runStartedAt or current time
            val eventTimestamp =
                event.startTime?.let { parseDateTime(it)?.toEpochMilli() }
                    ?: currentRun?.runStartedAt?.toEpochMilli()
                    ?: System.currentTimeMillis()

            // Check if this resource has already been processed for this phase (before saving)
            val isDuplicate =
                eventRunId?.let { runIdValue ->
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

            val entity =
                HarvestEventEntity(
                    eventType = event.phase.name,
                    dataSourceId = dataSourceId,
                    runId = eventRunId,
                    dataType = dataType,
                    dataSourceUrl = event.dataSourceUrl?.toString(),
                    acceptHeader = event.acceptHeader?.toString(),
                    fdkId = event.fdkId?.toString(),
                    resourceUri = event.resourceUri?.toString(),
                    timestamp = eventTimestamp,
                    startTime = event.startTime?.toString(),
                    endTime = event.endTime?.toString(),
                    errorMessage = event.errorMessage?.toString(),
                    changedResourcesCount = event.changedResourcesCount?.let { it.toInt() },
                    unchangedResourcesCount = event.unchangedResourcesCount?.let { it.toInt() },
                    removedResourcesCount = event.removedResourcesCount?.let { it.toInt() },
                )
            harvestEventRepository.save(entity)

            // Update or create harvest run (consolidated state tracking)
            // Pass isDuplicate flag to avoid double-counting
            updateHarvestRun(event, isDuplicate)

            logger.debug("Persisted harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}, fdkId=${event.fdkId}")
        } catch (e: Exception) {
            logger.error("Error persisting harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}", e)
            throw e
        }
    }

    private fun updateHarvestRun(
        event: HarvestEvent,
        isDuplicate: Boolean = false,
    ) {
        val dataSourceId = event.dataSourceId.toString()
        val dataType = event.dataType.name
        val runId = event.runId?.toString()

        // Find run by runId if available, otherwise fall back to finding current run
        val currentRun =
            if (runId != null) {
                harvestRunRepository.findByRunId(runId) ?: run {
                    // This shouldn't happen for trigger events (run should already exist)
                    // But handle it gracefully for other events
                    if (event.phase.name == "INITIATING") {
                        null // Trigger events should have the run created beforehand
                    } else {
                        // For non-trigger events, try to find current run
                        harvestRunRepository.findCurrentRun(dataSourceId, dataType)
                    }
                }
            } else {
                // Fallback: find the most recent run that's in progress (for backward compatibility)
                harvestRunRepository.findCurrentRun(dataSourceId, dataType)
            }

        if (currentRun != null) {
            val oldStatus = currentRun.status
            val updatedRun = updateRunWithEvent(currentRun, event, isDuplicate)
            val savedRun = harvestRunRepository.save(updatedRun)

            // Record metrics if status changed
            if (oldStatus != savedRun.status) {
                harvestMetricsService.recordRunCompleted(savedRun)
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
        }
    }

    private fun updateRunWithEvent(
        run: HarvestRunEntity,
        event: HarvestEvent,
        isDuplicate: Boolean = false,
    ): HarvestRunEntity {
        val startTime = event.startTime?.let { parseDateTime(it) }
        val endTime = event.endTime?.let { parseDateTime(it) }
        val currentPhase = event.phase.name
        // Use startTime if available, otherwise fallback to runStartedAt for timestamp
        val eventTimestamp = startTime ?: run.runStartedAt

        // Calculate resource counts
        val totalResources = calculateTotalResources(event, run)
        val processedResources = calculateProcessedResources(event, run, totalResources, isDuplicate)
        val remainingResources = totalResources?.let { total -> processedResources?.let { processed -> total - processed } }

        var updatedRun =
            run.copy(
                currentPhase = currentPhase,
                phaseStartedAt = if (currentPhase != run.currentPhase) eventTimestamp else run.phaseStartedAt,
                lastEventTimestamp = eventTimestamp.toEpochMilli(),
                errorMessage = event.errorMessage?.toString() ?: run.errorMessage,
                totalResources = totalResources,
                processedResources = processedResources,
                remainingResources = remainingResources,
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
            updatedRun =
                updatedRun.copy(
                    totalResources =
                        event.changedResourcesCount?.toInt()?.plus(event.unchangedResourcesCount?.toInt() ?: 0)?.plus(
                            event.removedResourcesCount?.toInt() ?: 0,
                        ),
                    changedResourcesCount = event.changedResourcesCount?.toInt(),
                    unchangedResourcesCount = event.unchangedResourcesCount?.toInt(),
                    removedResourcesCount = event.removedResourcesCount?.toInt(),
                )
        }

        // Update status
        val status = determineStatus(event, run, updatedRun)
        updatedRun = updatedRun.copy(status = status)

        // Check if harvest is complete (when all phases have endTime)
        val isComplete = checkIfAllPhasesComplete(run.runId, event)
        if (isComplete && run.runEndedAt == null) {
            // Use the latest endTime from any phase as the run end time
            val latestEndTime = getLatestEndTime(run.runId) ?: endTime
            val totalDuration =
                if (run.runStartedAt != null && latestEndTime != null) {
                    ChronoUnit.MILLIS.between(run.runStartedAt, latestEndTime)
                } else {
                    null
                }

            updatedRun =
                updatedRun.copy(
                    runEndedAt = latestEndTime,
                    totalDurationMs = totalDuration,
                )
        }

        return updatedRun
    }

    private fun determineStatus(
        event: HarvestEvent,
        existingRun: HarvestRunEntity?,
        updatedRun: HarvestRunEntity,
    ): String {
        if (event.errorMessage != null) {
            return "FAILED"
        }
        // Check if all phases are complete
        if (checkIfAllPhasesComplete(updatedRun.runId, event)) {
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

        // Check each required phase has an event with endTime
        return requiredPhases.all { phase ->
            if (phase == currentEvent.phase.name) {
                // For current phase, check if current event has endTime
                currentEvent.endTime != null
            } else {
                // For other phases, check database for events with endTime
                harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(runId, phase).isNotEmpty()
            }
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
        isDuplicate: Boolean = false,
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
        if (event.phase.name in resourceProcessingPhases && !isDuplicate) {
            val runId = event.runId?.toString() ?: existingRun?.runId
            if (runId != null) {
                // Check if this resource has now completed all phases
                val resourceIdentifier = event.fdkId?.toString() ?: event.resourceUri?.toString()
                if (resourceIdentifier != null) {
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

                    // Get unique phases that this resource has events for
                    val completedPhases = resourceEvents.map { it.eventType }.toSet()

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

    private fun countResourcesWithAllPhases(
        runId: String,
        requiredPhases: List<String>,
    ): Int {
        // Get all events for this run that are in the resource processing phases
        val allResourceEvents =
            requiredPhases.flatMap { phase ->
                harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(runId, phase)
            }

        // Group by resource identifier (fdkId takes precedence over resourceUri)
        val resourcesByFdkId =
            allResourceEvents
                .filter { it.fdkId != null }
                .groupBy { it.fdkId!! }
                .filter { (_, events) ->
                    // Check if this resource has events for all required phases
                    events.map { it.eventType }.toSet().containsAll(requiredPhases)
                }.keys

        // Resources identified by URI (excluding those already counted by fdkId)
        val fdkIds = resourcesByFdkId.toSet()
        val resourcesByUri =
            allResourceEvents
                .filter { it.resourceUri != null && (it.fdkId == null || !fdkIds.contains(it.fdkId)) }
                .groupBy { it.resourceUri!! }
                .filter { (_, events) ->
                    // Check if this resource has events for all required phases
                    events.map { it.eventType }.toSet().containsAll(requiredPhases)
                }.keys

        return resourcesByFdkId.size + resourcesByUri.size
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
                        java.time.OffsetDateTime.from(temporalAccessor).toInstant()
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
                        changedResourcesCount = run.changedResourcesCount,
                        unchangedResourcesCount = run.unchangedResourcesCount,
                        removedResourcesCount = run.removedResourcesCount,
                        status = run.status,
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
                        ),
                    status = run.status,
                    errorMessage = run.errorMessage,
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
                            ),
                        status = run.status,
                        errorMessage = run.errorMessage,
                    ),
                    HttpStatus.OK,
                )
            }
        } catch (e: Exception) {
            logger.error("Error getting harvest run for runId: $runId", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getHarvestRuns(
        dataSourceId: String,
        dataType: String? = null,
        limit: Int = 50,
    ): List<HarvestRunDetails> =
        try {
            val runs =
                if (dataType != null) {
                    harvestRunRepository.findByDataSourceIdAndDataTypeOrderByRunStartedAtDesc(dataSourceId, dataType)
                } else {
                    harvestRunRepository.findByDataSourceIdOrderByRunStartedAtDesc(dataSourceId)
                }

            runs.take(limit).map { run ->
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
                        ),
                    status = run.status,
                    errorMessage = run.errorMessage,
                )
            }
        } catch (e: Exception) {
            logger.error("Error getting harvest runs for dataSourceId: $dataSourceId, dataType: $dataType", e)
            emptyList()
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
}
