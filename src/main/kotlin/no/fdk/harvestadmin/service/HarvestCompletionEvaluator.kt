package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.model.PhaseCompletion
import no.fdk.harvestadmin.model.RunCompletionStatus
import no.fdk.harvestadmin.repository.HarvestEventRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Evaluates whether all required harvest phases are complete for a run.
 *
 * Rules:
 * - For phases without per-resource identifiers (e.g. HARVESTING), we only
 *   require at least one successful event (endTime != null, no error).
 * - For resource-based phases, when an expected resource count is known
 *   (changed + removed > 0), we require that the number of completed
 *   resources is at least the expected count. More is allowed
 *   (retries/duplicates), fewer will block completion.
 * - Optional phases (AI_SEARCH_PROCESSING, SPARQL_PROCESSING) do not block
 *   completion when there are no events at all for that phase in the run.
 *   If events do exist, they behave like required phases.
 */
@Service
class HarvestCompletionEvaluator(
    private val harvestEventRepository: HarvestEventRepository,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun evaluate(run: HarvestRunEntity): RunCompletionStatus = evaluate(run, calculatePhaseEventCounts(run.runId))

    fun evaluate(
        run: HarvestRunEntity,
        phaseEventCounts: Map<String, Long>,
    ): RunCompletionStatus {
        val expectedResourceCount = (run.changedResourcesCount ?: 0) + (run.removedResourcesCount ?: 0)
        val hasExplicitResourceCounts =
            run.changedResourcesCount != null ||
                run.removedResourcesCount != null ||
                run.totalResources != null

        val phasesWithoutResourceIds = listOf(HarvestPhaseConfig.HARVESTING_PHASE)

        val phaseCompletions = mutableListOf<PhaseCompletion>()
        var allRequiredComplete = true

        // When there are zero resources to process a successful HARVESTING phase is enough to consider the run completed.
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

            logBlockingPhasesIfNeeded(run.runId, allRequiredComplete, phaseCompletions)

            return RunCompletionStatus(
                allPhasesComplete = allRequiredComplete,
                phases = phaseCompletions,
            )
        }

        val completedResourcesByPhase =
            harvestEventRepository
                .countCompletedResourcesPerPhase(run.runId, HarvestPhaseConfig.resourceProcessingPhases)
                .associate { (eventType, count) -> eventType as String to (count as Number).toInt() }

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
                // For phases with resource identifiers, the SQL aggregate already counts resources
                // whose latest event is completed.
                val completedResources = completedResourcesByPhase[phase] ?: 0
                val hasAnyCompleted = completedResources > 0

                // Determine whether this phase is effectively required for this run.
                val isOptionalAndUnused = isOptionalByConfig && (phaseEventCounts[phase] ?: 0L) == 0L
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

        logBlockingPhasesIfNeeded(run.runId, allRequiredComplete, phaseCompletions)

        return RunCompletionStatus(
            allPhasesComplete = allRequiredComplete,
            phases = phaseCompletions,
        )
    }

    fun calculatePhaseEventCounts(runId: String): Map<String, Long> {
        val countsByPhase =
            harvestEventRepository
                .countEventsByPhase(runId)
                .associate { (eventType, count) -> eventType as String to count as Long }

        return HarvestPhaseConfig.allPhasesForEventCounts.associateWith { phase -> countsByPhase[phase] ?: 0L }
    }

    fun countResourcesWithAllPhases(
        runId: String,
        requiredPhases: List<String>,
        phaseEventCounts: Map<String, Long>,
    ): Int {
        val effectiveRequiredPhases =
            requiredPhases.filter { phase ->
                if (phase in HarvestPhaseConfig.optionalPhases) {
                    (phaseEventCounts[phase] ?: 0L) > 0L
                } else {
                    true
                }
            }

        if (effectiveRequiredPhases.isEmpty()) {
            return 0
        }

        // SQL count of the resources that have a completed event in every effective phase.
        return harvestEventRepository
            .countResourcesCompletedInAllPhases(
                runId,
                effectiveRequiredPhases,
                effectiveRequiredPhases.size.toLong(),
            ).toInt()
    }

    private fun logBlockingPhasesIfNeeded(
        runId: String,
        allRequiredComplete: Boolean,
        phaseCompletions: List<PhaseCompletion>,
    ) {
        if (!allRequiredComplete) {
            val blockingPhases =
                phaseCompletions
                    .filter { it.required && !it.complete }
                    .joinToString { "${it.phase}(expected=${it.expectedResources ?: "?"}, completed=${it.completedResources})" }
            logger.debug(
                "Run $runId not yet COMPLETED. Blocking phases: $blockingPhases",
            )
        }
    }
}
