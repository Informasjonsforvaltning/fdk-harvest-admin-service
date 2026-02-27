package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.swagger.v3.oas.annotations.media.Schema
import java.time.Instant

@Schema(description = "Request body for starting a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class StartHarvestRequest(
    @field:Schema(description = "If true, mark all resources for the data source as deleted (INITIATING only)")
    val removeAll: Boolean? = null,
    @field:Schema(description = "If true, force an update even if resources have not changed (INITIATING only)")
    val forced: Boolean? = null,
)

@Schema(description = "Current state of a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestCurrentState(
    @field:Schema(description = "Data source identifier", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String,
    @field:Schema(
        description = "Data type",
        example = "dataset",
        allowableValues = ["concept", "dataset", "informationmodel", "dataservice", "publicService", "event"],
    )
    val dataType: String,
    @field:Schema(description = "Current harvest phase", example = "HARVESTING")
    val currentPhase: String?,
    @field:Schema(description = "When the current phase started")
    val phaseStartedAt: Instant?,
    @field:Schema(description = "Timestamp of the last event")
    val lastEventTimestamp: Long?,
    @field:Schema(description = "Error message if harvest failed")
    val errorMessage: String?,
    @field:Schema(description = "Total number of resources")
    val totalResources: Int?,
    @field:Schema(description = "Number of processed resources")
    val processedResources: Int?,
    @field:Schema(description = "Number of remaining resources")
    val remainingResources: Int?,
    @field:Schema(description = "Event counts per phase")
    val phaseEventCounts: PhaseEventCounts?,
    @field:Schema(description = "Number of changed resources")
    val changedResourcesCount: Int?,
    @field:Schema(description = "Number of removed resources")
    val removedResourcesCount: Int?,
    @field:Schema(description = "If true, all resources for the data source are marked as deleted (INITIATING only)")
    val removeAll: Boolean? = null,
    @field:Schema(description = "If true, forces an update even if resources have not changed (INITIATING only)")
    val forced: Boolean? = null,
    @field:Schema(description = "Harvest status", example = "IN_PROGRESS", allowableValues = ["IN_PROGRESS", "COMPLETED", "FAILED"])
    val status: String, // IN_PROGRESS, DONE, ERROR
    @field:Schema(description = "When the harvest run was created")
    val createdAt: Instant?,
    @field:Schema(description = "When the harvest run was last updated")
    val updatedAt: Instant?,
)

@Schema(description = "Performance metrics for harvest runs")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestPerformanceMetrics(
    @field:Schema(description = "Data source identifier (null for global metrics)", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String? = null,
    @field:Schema(description = "Data type (null for global metrics)", example = "dataset")
    val dataType: String? = null,
    @field:Schema(description = "Total number of runs", example = "10")
    val totalRuns: Int,
    @field:Schema(description = "Number of completed runs", example = "8")
    val completedRuns: Int,
    @field:Schema(description = "Number of failed runs", example = "2")
    val failedRuns: Int,
    @field:Schema(description = "Average total duration in milliseconds", example = "5000.0")
    val averageTotalDurationMs: Double?,
    @field:Schema(description = "Average harvest duration in milliseconds", example = "2000.0")
    val averageHarvestDurationMs: Double?,
    @field:Schema(description = "Average reasoning duration in milliseconds", example = "1000.0")
    val averageReasoningDurationMs: Double?,
    @field:Schema(description = "Average RDF parsing duration in milliseconds", example = "500.0")
    val averageRdfParsingDurationMs: Double?,
    @field:Schema(description = "Average search processing duration in milliseconds", example = "300.0")
    val averageSearchProcessingDurationMs: Double?,
    @field:Schema(description = "Average AI search processing duration in milliseconds", example = "200.0")
    val averageAiSearchProcessingDurationMs: Double?,
    @field:Schema(description = "Average API processing duration in milliseconds", example = "100.0")
    val averageApiProcessingDurationMs: Double?,
    @field:Schema(description = "Average SPARQL processing duration in milliseconds", example = "50.0")
    val averageSparqlProcessingDurationMs: Double?,
    @field:Schema(description = "Start of the metrics period")
    val periodStart: Instant,
    @field:Schema(description = "End of the metrics period")
    val periodEnd: Instant,
)

@Schema(description = "Detailed information about a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestRunDetails(
    @field:Schema(description = "Run ID (UUID)", example = "123e4567-e89b-12d3-a456-426614174000")
    val runId: String,
    @field:Schema(description = "Data source identifier", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String,
    @field:Schema(description = "Data type", example = "dataset")
    val dataType: String,
    @field:Schema(description = "When the harvest run started")
    val runStartedAt: Instant,
    @field:Schema(description = "When the harvest run ended (null if still in progress)")
    val runEndedAt: Instant?,
    @field:Schema(description = "Total duration in milliseconds", example = "5000")
    val totalDurationMs: Long?,
    @field:Schema(description = "Duration of each phase in milliseconds")
    val phaseDurations: PhaseDurations,
    @field:Schema(description = "Resource counts")
    val resourceCounts: ResourceCounts,
    @field:Schema(description = "If true, all resources for the data source were marked as deleted (INITIATING only)")
    val removeAll: Boolean? = null,
    @field:Schema(description = "If true, harvest was forced even if resources had not changed (INITIATING only)")
    val forced: Boolean? = null,
    @field:Schema(description = "Harvest status", example = "COMPLETED", allowableValues = ["IN_PROGRESS", "COMPLETED", "FAILED"])
    val status: String,
    @field:Schema(description = "Error message if harvest failed")
    val errorMessage: String?,
    @field:Schema(description = "When the harvest run was created")
    val createdAt: Instant?,
    @field:Schema(description = "When the harvest run was last updated")
    val updatedAt: Instant?,
    @field:Schema(
        description =
            "Per-phase completion details for this run. " +
                "Populated in particular when the run is not COMPLETED to show which phases or resources are missing.",
    )
    val completionStatus: RunCompletionStatus? = null,
)

@Schema(description = "Duration of each harvest phase in milliseconds")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PhaseDurations(
    @field:Schema(description = "Initialization duration", example = "100")
    val initDurationMs: Long?,
    @field:Schema(description = "Harvesting duration", example = "2000")
    val harvestDurationMs: Long?,
    @field:Schema(description = "Reasoning duration", example = "1000")
    val reasoningDurationMs: Long?,
    @field:Schema(description = "RDF parsing duration", example = "500")
    val rdfParsingDurationMs: Long?,
    @field:Schema(description = "Search processing duration", example = "300")
    val searchProcessingDurationMs: Long?,
    @field:Schema(description = "AI search processing duration", example = "200")
    val aiSearchProcessingDurationMs: Long?,
    @field:Schema(description = "API processing duration", example = "100")
    val apiProcessingDurationMs: Long?,
    @field:Schema(description = "SPARQL processing duration", example = "50")
    val sparqlProcessingDurationMs: Long?,
)

@Schema(description = "Resource counts for a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class ResourceCounts(
    @field:Schema(description = "Total number of resources", example = "100")
    val totalResources: Int?,
    @field:Schema(description = "Number of changed resources", example = "10")
    val changedResourcesCount: Int?,
    @field:Schema(description = "Number of removed resources", example = "5")
    val removedResourcesCount: Int?,
    @field:Schema(description = "Event counts per phase")
    val phaseEventCounts: PhaseEventCounts?,
)

@Schema(description = "Completion status for a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class RunCompletionStatus(
    @field:Schema(description = "Whether all required phases are complete for this run")
    val allPhasesComplete: Boolean,
    @field:Schema(description = "Per-phase completion details")
    val phases: List<PhaseCompletion>,
)

@Schema(description = "Per-phase completion details for a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PhaseCompletion(
    @field:Schema(description = "Harvest phase name", example = "RDF_PARSING")
    val phase: String,
    @field:Schema(description = "Whether this phase is required for completion. Optional phases with no events do not block completion.")
    val required: Boolean,
    @field:Schema(
        description =
            "Expected number of unique resources for this phase (changed + removed) when known. " +
                "For phases without per-resource identifiers (like HARVESTING) this is null.",
        example = "120",
    )
    val expectedResources: Int?,
    @field:Schema(
        description =
            "Number of unique resources whose latest event in this phase has completed successfully " +
                "(has endTime and no error).",
        example = "117",
    )
    val completedResources: Int,
    @field:Schema(description = "Whether this phase is considered complete for this run")
    val complete: Boolean,
)

@Schema(description = "Event counts per harvest phase")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PhaseEventCounts(
    @field:Schema(description = "Number of INITIATING events", example = "1")
    val initiatingEventsCount: Int?,
    @field:Schema(description = "Number of HARVESTING events", example = "1")
    val harvestingEventsCount: Int?,
    @field:Schema(description = "Number of REASONING events", example = "100")
    val reasoningEventsCount: Int?,
    @field:Schema(description = "Number of RDF_PARSING events", example = "100")
    val rdfParsingEventsCount: Int?,
    @field:Schema(description = "Number of RESOURCE_PROCESSING events", example = "100")
    val resourceProcessingEventsCount: Int?,
    @field:Schema(description = "Number of SEARCH_PROCESSING events", example = "100")
    val searchProcessingEventsCount: Int?,
    @field:Schema(description = "Number of AI_SEARCH_PROCESSING events", example = "100")
    val aiSearchProcessingEventsCount: Int?,
    @field:Schema(description = "Number of SPARQL_PROCESSING events", example = "100")
    val sparqlProcessingEventsCount: Int?,
)
