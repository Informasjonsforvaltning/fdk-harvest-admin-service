package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.temporal.ChronoUnit

@Service
class HarvestEventIngestionService(
    private val harvestEventRepository: HarvestEventRepository,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
    private val completionEvaluator: HarvestCompletionEvaluator,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun persistEvent(event: HarvestEvent) = persistEvents(listOf(event))

    /**
     * Persists a batch of harvest events in one transaction.
     */
    @Transactional
    fun persistEvents(events: List<HarvestEvent>) {
        val eventsByRunId = LinkedHashMap<String, MutableList<HarvestEvent>>()
        events.forEach { event ->
            val runId = event.runId
            if (runId == null) {
                logger.warn("Cannot process harvest event: runId is required. phase=${event.phase}")
            } else {
                eventsByRunId.getOrPut(runId) { mutableListOf() }.add(event)
            }
        }

        eventsByRunId.forEach { (runId, runEvents) -> persistRunEvents(runId, runEvents) }
    }

    private fun persistRunEvents(runId: String, events: List<HarvestEvent>) {
        try {
            val currentRun = harvestRunRepository.findByRunId(runId)

            val persistedEvents =
                events.filter { event ->
                    // Use dataSourceId from event if available (INITIATING phase), otherwise from the found run
                    val effectiveDataSourceId = event.dataSourceId ?: currentRun?.dataSourceId
                    if (effectiveDataSourceId == null) {
                        logger.warn("Cannot process harvest event: no dataSourceId available. phase=${event.phase}, runId=$runId")
                        false
                    } else {
                        harvestEventRepository.save(event.toEntity(runId, effectiveDataSourceId))
                        logger.debug("Persisted harvest event: phase=${event.phase}, runId=$runId, fdkId=${event.fdkId}")
                        true
                    }
                }

            if (persistedEvents.isEmpty()) {
                return
            }

            // Only update the run if it exists
            if (currentRun != null) {
                updateHarvestRun(persistedEvents, currentRun)
            } else {
                logger.debug("Skipping run update: harvest run not found for runId: $runId")
            }
        } catch (e: Exception) {
            logger.error("Error persisting ${events.size} harvest event(s) for runId=$runId", e)
            throw e
        }
    }

    private fun HarvestEvent.toEntity(runId: String, dataSourceId: String): HarvestEventEntity = HarvestEventEntity(
        eventType = phase.name,
        dataSourceId = dataSourceId,
        runId = runId,
        dataType = dataType.name,
        dataSourceUrl = dataSourceUrl,
        acceptHeader = acceptHeader,
        fdkId = fdkId,
        resourceUri = resourceUri,
        startTime = startTime,
        endTime = endTime,
        errorMessage = errorMessage,
        changedResourcesCount = changedResourcesCount,
        removedResourcesCount = removedResourcesCount,
    )

    private fun updateHarvestRun(events: List<HarvestEvent>, currentRun: HarvestRunEntity) {
        val oldStatus = currentRun.status
        val updatedRun = updateRunWithEvents(currentRun, events)
        val savedRun = harvestRunRepository.save(updatedRun)

        // Only record ongoing metrics if the run was still IN_PROGRESS when the batch arrived
        // This prevents recording metrics for late-arriving events after a run has completed
        if (oldStatus == "IN_PROGRESS") {
            val processedResources = savedRun.processedResources
            events.forEach { event ->
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
                if (processedResources != null &&
                    processedResources > 0 &&
                    event.phase.name in HarvestPhaseConfig.resourceProcessingPhases
                ) {
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

    private fun updateRunWithEvents(run: HarvestRunEntity, events: List<HarvestEvent>): HarvestRunEntity {
        val lastEvent = events.last()
        var updatedRun = events.fold(run) { acc, event -> applyEventFields(acc, event) }

        // Per-phase total event counts (single grouped query), reused by completion and processed-count logic.
        val phaseEventCounts = completionEvaluator.calculatePhaseEventCounts(run.runId)

        val totalResources = updatedRun.totalResources
        val processedResources =
            calculateProcessedResources(
                run.runId,
                totalResources,
                updatedRun.processedResources,
                events.any { it.phase.name in HarvestPhaseConfig.resourceProcessingPhases },
                phaseEventCounts,
            )
        val remainingResources = totalResources?.let { total -> processedResources?.let { processed -> total - processed } }

        updatedRun =
            updatedRun
                .copy(processedResources = processedResources, remainingResources = remainingResources)
                .withPhaseEventCounts(phaseEventCounts)

        // Evaluate completion across phases using the latest in-memory run state
        val completionStatus = completionEvaluator.evaluate(updatedRun, phaseEventCounts)

        // Determine status based on error and completion state.
        val status =
            if (lastEvent.errorMessage != null) {
                "FAILED"
            } else if (completionStatus.allPhasesComplete) {
                "COMPLETED"
            } else {
                existingStatusOrDefault(updatedRun)
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
                    getLatestEndTime(run.runId) ?: lastEvent.endTime?.let { parseDateTime(it) }
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
        if (finalUpdatedRun.status == "COMPLETED" && lastEvent.errorMessage == null) {
            finalUpdatedRun = finalUpdatedRun.copy(errorMessage = null)
        }

        return finalUpdatedRun
    }

    private fun applyEventFields(run: HarvestRunEntity, event: HarvestEvent): HarvestRunEntity {
        val startTime = event.startTime?.let { parseDateTime(it) }
        val endTime = event.endTime?.let { parseDateTime(it) }
        val currentPhase = event.phase.name
        val eventTimestamp = startTime ?: run.runStartedAt

        val removeAll =
            if (event.phase.name == HarvestPhaseConfig.INITIATING_PHASE) event.removeAll else run.removeAll
        val forced =
            if (event.phase.name == HarvestPhaseConfig.INITIATING_PHASE) event.forced else run.forced

        var updatedRun =
            run.copy(
                currentPhase = currentPhase,
                phaseStartedAt = if (currentPhase != run.currentPhase) eventTimestamp else run.phaseStartedAt,
                lastEventTimestamp = eventTimestamp.toEpochMilli(),
                errorMessage = event.errorMessage ?: run.errorMessage,
                removeAll = removeAll,
                forced = forced,
                status = if (event.errorMessage != null) "FAILED" else run.status,
                updatedAt = Instant.now(),
            )

        updatedRun = applyPhaseDurations(run, updatedRun, currentPhase, startTime, endTime)

        if (event.changedResourcesCount != null || event.removedResourcesCount != null) {
            updatedRun =
                updatedRun.copy(
                    totalResources = (event.changedResourcesCount ?: 0) + (event.removedResourcesCount ?: 0),
                    changedResourcesCount = event.changedResourcesCount,
                    removedResourcesCount = event.removedResourcesCount,
                )
        }

        return updatedRun
    }

    /**
     * Accumulate phase duration fields from start/end times.
     */
    private fun applyPhaseDurations(
        run: HarvestRunEntity,
        updatedRun: HarvestRunEntity,
        phase: String,
        startTime: Instant?,
        endTime: Instant?,
    ): HarvestRunEntity {
        if (phase == HarvestPhaseConfig.HARVESTING_PHASE) {
            var result = updatedRun
            if (startTime != null) {
                result =
                    result.copy(
                        initDurationMs = ChronoUnit.MILLIS.between(run.runStartedAt, startTime),
                    )
            }
            if (startTime != null && endTime != null) {
                result =
                    result.copy(
                        harvestDurationMs = ChronoUnit.MILLIS.between(startTime, endTime),
                    )
            }
            return result
        }

        if (startTime == null || endTime == null) {
            return updatedRun
        }

        val deltaMs = ChronoUnit.MILLIS.between(startTime, endTime)
        return when (phase) {
            "REASONING" -> {
                updatedRun.copy(reasoningDurationMs = (run.reasoningDurationMs ?: 0L) + deltaMs)
            }

            "RDF_PARSING" -> {
                updatedRun.copy(rdfParsingDurationMs = (run.rdfParsingDurationMs ?: 0L) + deltaMs)
            }

            "SEARCH_PROCESSING" -> {
                updatedRun.copy(searchProcessingDurationMs = (run.searchProcessingDurationMs ?: 0L) + deltaMs)
            }

            "AI_SEARCH_PROCESSING" -> {
                updatedRun.copy(aiSearchProcessingDurationMs = (run.aiSearchProcessingDurationMs ?: 0L) + deltaMs)
            }

            "RESOURCE_PROCESSING" -> {
                updatedRun.copy(apiProcessingDurationMs = (run.apiProcessingDurationMs ?: 0L) + deltaMs)
            }

            "SPARQL_PROCESSING" -> {
                updatedRun.copy(sparqlProcessingDurationMs = (run.sparqlProcessingDurationMs ?: 0L) + deltaMs)
            }

            else -> {
                updatedRun
            }
        }
    }

    private fun HarvestRunEntity.withPhaseEventCounts(phaseEventCounts: Map<String, Long>): HarvestRunEntity = copy(
        initiatingEventsCount = phaseEventCounts[HarvestPhaseConfig.INITIATING_PHASE]?.toInt(),
        harvestingEventsCount = phaseEventCounts[HarvestPhaseConfig.HARVESTING_PHASE]?.toInt(),
        reasoningEventsCount = phaseEventCounts["REASONING"]?.toInt(),
        rdfParsingEventsCount = phaseEventCounts["RDF_PARSING"]?.toInt(),
        resourceProcessingEventsCount = phaseEventCounts["RESOURCE_PROCESSING"]?.toInt(),
        searchProcessingEventsCount = phaseEventCounts["SEARCH_PROCESSING"]?.toInt(),
        aiSearchProcessingEventsCount = phaseEventCounts["AI_SEARCH_PROCESSING"]?.toInt(),
        sparqlProcessingEventsCount = phaseEventCounts["SPARQL_PROCESSING"]?.toInt(),
    )

    private fun existingStatusOrDefault(run: HarvestRunEntity): String = run.status.ifBlank { "IN_PROGRESS" }

    private fun getLatestEndTime(runId: String): Instant? = HarvestPhaseConfig.allPhasesInCompletionOrder
        .flatMap { phase ->
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(runId, phase)
        }.mapNotNull { event ->
            event.endTime?.let { parseDateTime(it) }
        }.maxOrNull()

    private fun calculateProcessedResources(
        runId: String,
        totalResources: Int?,
        existingProcessed: Int?,
        hasResourcePhaseEvent: Boolean,
        phaseEventCounts: Map<String, Long>,
    ): Int? {
        // If total resources is not set yet, we can't calculate processed.
        if (totalResources == null || !hasResourcePhaseEvent) {
            return existingProcessed
        }

        // Use the same effective phase logic as completion: optional phases without events
        // are not required when counting fully processed resources for the run.
        val resourcesWithAllPhases =
            completionEvaluator.countResourcesWithAllPhases(runId, HarvestPhaseConfig.resourceProcessingPhases, phaseEventCounts)
        return minOf(totalResources, resourcesWithAllPhases)
    }

    private fun parseDateTime(dateString: String): Instant? {
        val parsers: List<() -> Instant> =
            listOf(
                { Instant.parse(dateString) },
                {
                    // e.g. "2025-12-11 13:21:38 +0100"
                    java.time.format.DateTimeFormatter
                        .ofPattern("yyyy-MM-dd HH:mm:ss Z")
                        .parse(dateString)
                        .let {
                            java.time.OffsetDateTime
                                .from(it)
                                .toInstant()
                        }
                },
                {
                    // Local wall time without zone
                    java.time.LocalDateTime
                        .parse(
                            dateString,
                            java.time.format.DateTimeFormatter
                                .ofPattern("yyyy-MM-dd HH:mm:ss"),
                        ).atZone(java.time.ZoneId.systemDefault())
                        .toInstant()
                },
            )

        for (parser in parsers) {
            try {
                return parser()
            } catch (_: Exception) {
                // try next format
            }
        }

        logger.warn("Could not parse date string: $dateString")
        return null
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
