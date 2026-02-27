package no.fdk.harvestadmin.service

/**
 * Central configuration for harvest phases and completion rules.
 *
 * This defines which phases participate in run completion checks and
 * which phases are considered optional. Optional phases do not block
 * completion when there are no events for that phase in a run, but if
 * events do exist they behave like required phases (counts must match).
 */
object HarvestPhaseConfig {
    const val HARVESTING_PHASE: String = "HARVESTING"

    /**
     * Phases that operate on individual resources and are used to
     * determine when a resource is fully processed.
     */
    val resourceProcessingPhases: List<String> =
        listOf(
            "REASONING",
            "RDF_PARSING",
            "RESOURCE_PROCESSING",
            "SEARCH_PROCESSING",
            "AI_SEARCH_PROCESSING",
            "SPARQL_PROCESSING",
        )

    /**
     * All phases that are considered when determining overall run
     * completion, in logical order.
     */
    val allPhasesInCompletionOrder: List<String> =
        listOf(HARVESTING_PHASE) + resourceProcessingPhases

    /**
     * Phases that are optional for completion when there are no events
     * for that phase in a given run.
     *
     * If a run does have events for an optional phase, that phase is
     * treated as required and its resource counts must match.
     *
     * This keeps runs for pipelines that do not emit AI or SPARQL events
     * from being stuck in IN_PROGRESS, while still enforcing consistency
     * when those phases are in use.
     */
    val optionalPhases: Set<String> =
        setOf(
            "AI_SEARCH_PROCESSING",
            "SPARQL_PROCESSING",
        )
}
