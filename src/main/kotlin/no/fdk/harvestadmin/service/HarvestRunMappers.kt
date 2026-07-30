package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.model.HarvestCurrentState
import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.model.PhaseDurations
import no.fdk.harvestadmin.model.PhaseEventCounts
import no.fdk.harvestadmin.model.ResourceCounts
import no.fdk.harvestadmin.model.RunCompletionStatus
import java.time.Instant

fun HarvestRunEntity.toPhaseEventCounts(): PhaseEventCounts =
    PhaseEventCounts(
        initiatingEventsCount = initiatingEventsCount,
        harvestingEventsCount = harvestingEventsCount,
        reasoningEventsCount = reasoningEventsCount,
        rdfParsingEventsCount = rdfParsingEventsCount,
        resourceProcessingEventsCount = resourceProcessingEventsCount,
        searchProcessingEventsCount = searchProcessingEventsCount,
        aiSearchProcessingEventsCount = aiSearchProcessingEventsCount,
        sparqlProcessingEventsCount = sparqlProcessingEventsCount,
    )

fun HarvestRunEntity.toPhaseDurations(): PhaseDurations =
    PhaseDurations(
        initDurationMs = initDurationMs,
        harvestDurationMs = harvestDurationMs,
        reasoningDurationMs = reasoningDurationMs,
        rdfParsingDurationMs = rdfParsingDurationMs,
        searchProcessingDurationMs = searchProcessingDurationMs,
        aiSearchProcessingDurationMs = aiSearchProcessingDurationMs,
        apiProcessingDurationMs = apiProcessingDurationMs,
        sparqlProcessingDurationMs = sparqlProcessingDurationMs,
    )

fun HarvestRunEntity.toResourceCounts(): ResourceCounts =
    ResourceCounts(
        totalResources = totalResources,
        changedResourcesCount = changedResourcesCount,
        removedResourcesCount = removedResourcesCount,
        phaseEventCounts = toPhaseEventCounts(),
    )

fun HarvestRunEntity.toHarvestRunDetails(completionStatus: RunCompletionStatus? = null): HarvestRunDetails =
    HarvestRunDetails(
        runId = runId,
        dataSourceId = dataSourceId,
        dataType = dataType,
        runStartedAt = runStartedAt,
        runEndedAt = runEndedAt,
        totalDurationMs = totalDurationMs,
        phaseDurations = toPhaseDurations(),
        resourceCounts = toResourceCounts(),
        removeAll = removeAll,
        forced = forced,
        status = status,
        errorMessage = errorMessage,
        createdAt = createdAt,
        updatedAt = updatedAt,
        completionStatus = completionStatus,
    )

fun HarvestRunEntity.toHarvestCurrentState(): HarvestCurrentState =
    HarvestCurrentState(
        dataSourceId = dataSourceId,
        dataType = dataType,
        currentPhase = currentPhase,
        phaseStartedAt = phaseStartedAt,
        lastEventTimestamp = lastEventTimestamp,
        errorMessage = errorMessage,
        totalResources = totalResources,
        processedResources = processedResources,
        remainingResources = remainingResources,
        phaseEventCounts = toPhaseEventCounts(),
        changedResourcesCount = changedResourcesCount,
        removedResourcesCount = removedResourcesCount,
        removeAll = removeAll,
        forced = forced,
        status = status,
        createdAt = createdAt,
        updatedAt = updatedAt,
    )

fun buildPerformanceMetrics(
    runs: List<HarvestRunEntity>,
    dataSourceId: String?,
    dataType: String?,
): HarvestPerformanceMetrics {
    val successfulRuns = runs.filter { it.status == "COMPLETED" && it.errorMessage == null }
    val completedRuns = runs.filter { it.status == "COMPLETED" }
    val failedRuns = runs.filter { it.status == "FAILED" }

    return HarvestPerformanceMetrics(
        dataSourceId = dataSourceId,
        dataType = dataType,
        totalRuns = runs.size,
        completedRuns = completedRuns.size,
        failedRuns = failedRuns.size,
        averageTotalDurationMs = averageOf(successfulRuns) { it.totalDurationMs?.toDouble() },
        averageHarvestDurationMs = averageOf(successfulRuns) { it.harvestDurationMs?.toDouble() },
        averageReasoningDurationMs = averageOf(successfulRuns) { it.reasoningDurationMs?.toDouble() },
        averageRdfParsingDurationMs = averageOf(successfulRuns) { it.rdfParsingDurationMs?.toDouble() },
        averageSearchProcessingDurationMs = averageOf(successfulRuns) { it.searchProcessingDurationMs?.toDouble() },
        averageAiSearchProcessingDurationMs = averageOf(successfulRuns) { it.aiSearchProcessingDurationMs?.toDouble() },
        averageApiProcessingDurationMs = averageOf(successfulRuns) { it.apiProcessingDurationMs?.toDouble() },
        averageSparqlProcessingDurationMs = averageOf(successfulRuns) { it.sparqlProcessingDurationMs?.toDouble() },
        periodStart = runs.minOfOrNull { it.runStartedAt } ?: Instant.now(),
        periodEnd = runs.maxOfOrNull { it.runStartedAt } ?: Instant.now(),
    )
}

private fun <T> averageOf(
    items: List<T>,
    extractor: (T) -> Double?,
): Double? {
    val values = items.mapNotNull(extractor)
    return if (values.isNotEmpty()) values.average() else null
}
