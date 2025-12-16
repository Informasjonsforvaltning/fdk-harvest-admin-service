package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.swagger.v3.oas.annotations.media.Schema
import java.time.Instant

@Schema(description = "Current state of a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestCurrentState(
    @Schema(description = "Data source identifier", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String,
    @Schema(
        description = "Data type",
        example = "dataset",
        allowableValues = ["concept", "dataset", "informationmodel", "dataservice", "publicService", "event"],
    )
    val dataType: String,
    @Schema(description = "Current harvest phase", example = "HARVESTING")
    val currentPhase: String?,
    @Schema(description = "When the current phase started")
    val phaseStartedAt: Instant?,
    @Schema(description = "Timestamp of the last event")
    val lastEventTimestamp: Long?,
    @Schema(description = "Error message if harvest failed")
    val errorMessage: String?,
    @Schema(description = "Total number of resources")
    val totalResources: Int?,
    @Schema(description = "Number of processed resources")
    val processedResources: Int?,
    @Schema(description = "Number of remaining resources")
    val remainingResources: Int?,
    @Schema(description = "Event counts per phase")
    val phaseEventCounts: PhaseEventCounts?,
    @Schema(description = "Number of changed resources")
    val changedResourcesCount: Int?,
    @Schema(description = "Number of unchanged resources")
    val unchangedResourcesCount: Int?,
    @Schema(description = "Number of removed resources")
    val removedResourcesCount: Int?,
    @Schema(description = "Harvest status", example = "IN_PROGRESS", allowableValues = ["IN_PROGRESS", "COMPLETED", "FAILED"])
    val status: String, // IN_PROGRESS, DONE, ERROR
    @Schema(description = "When the harvest run was created")
    val createdAt: Instant?,
    @Schema(description = "When the harvest run was last updated")
    val updatedAt: Instant?,
)

@Schema(description = "Performance metrics for harvest runs")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestPerformanceMetrics(
    @Schema(description = "Data source identifier (null for global metrics)", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String? = null,
    @Schema(description = "Data type (null for global metrics)", example = "dataset")
    val dataType: String? = null,
    @Schema(description = "Total number of runs", example = "10")
    val totalRuns: Int,
    @Schema(description = "Number of completed runs", example = "8")
    val completedRuns: Int,
    @Schema(description = "Number of failed runs", example = "2")
    val failedRuns: Int,
    @Schema(description = "Average total duration in milliseconds", example = "5000.0")
    val averageTotalDurationMs: Double?,
    @Schema(description = "Average harvest duration in milliseconds", example = "2000.0")
    val averageHarvestDurationMs: Double?,
    @Schema(description = "Average reasoning duration in milliseconds", example = "1000.0")
    val averageReasoningDurationMs: Double?,
    @Schema(description = "Average RDF parsing duration in milliseconds", example = "500.0")
    val averageRdfParsingDurationMs: Double?,
    @Schema(description = "Average search processing duration in milliseconds", example = "300.0")
    val averageSearchProcessingDurationMs: Double?,
    @Schema(description = "Average AI search processing duration in milliseconds", example = "200.0")
    val averageAiSearchProcessingDurationMs: Double?,
    @Schema(description = "Average API processing duration in milliseconds", example = "100.0")
    val averageApiProcessingDurationMs: Double?,
    @Schema(description = "Average SPARQL processing duration in milliseconds", example = "50.0")
    val averageSparqlProcessingDurationMs: Double?,
    @Schema(description = "Start of the metrics period")
    val periodStart: Instant,
    @Schema(description = "End of the metrics period")
    val periodEnd: Instant,
)

@Schema(description = "Detailed information about a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestRunDetails(
    @Schema(description = "Run ID (UUID)", example = "123e4567-e89b-12d3-a456-426614174000")
    val runId: String,
    @Schema(description = "Data source identifier", example = "123e4567-e89b-12d3-a456-426614174000")
    val dataSourceId: String,
    @Schema(description = "Data type", example = "dataset")
    val dataType: String,
    @Schema(description = "When the harvest run started")
    val runStartedAt: Instant,
    @Schema(description = "When the harvest run ended (null if still in progress)")
    val runEndedAt: Instant?,
    @Schema(description = "Total duration in milliseconds", example = "5000")
    val totalDurationMs: Long?,
    @Schema(description = "Duration of each phase in milliseconds")
    val phaseDurations: PhaseDurations,
    @Schema(description = "Resource counts")
    val resourceCounts: ResourceCounts,
    @Schema(description = "Harvest status", example = "COMPLETED", allowableValues = ["IN_PROGRESS", "COMPLETED", "FAILED"])
    val status: String,
    @Schema(description = "Error message if harvest failed")
    val errorMessage: String?,
    @Schema(description = "When the harvest run was created")
    val createdAt: Instant?,
    @Schema(description = "When the harvest run was last updated")
    val updatedAt: Instant?,
)

@Schema(description = "Duration of each harvest phase in milliseconds")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PhaseDurations(
    @Schema(description = "Initialization duration", example = "100")
    val initDurationMs: Long?,
    @Schema(description = "Harvesting duration", example = "2000")
    val harvestDurationMs: Long?,
    @Schema(description = "Reasoning duration", example = "1000")
    val reasoningDurationMs: Long?,
    @Schema(description = "RDF parsing duration", example = "500")
    val rdfParsingDurationMs: Long?,
    @Schema(description = "Search processing duration", example = "300")
    val searchProcessingDurationMs: Long?,
    @Schema(description = "AI search processing duration", example = "200")
    val aiSearchProcessingDurationMs: Long?,
    @Schema(description = "API processing duration", example = "100")
    val apiProcessingDurationMs: Long?,
    @Schema(description = "SPARQL processing duration", example = "50")
    val sparqlProcessingDurationMs: Long?,
)

@Schema(description = "Resource counts for a harvest run")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class ResourceCounts(
    @Schema(description = "Total number of resources", example = "100")
    val totalResources: Int?,
    @Schema(description = "Number of changed resources", example = "10")
    val changedResourcesCount: Int?,
    @Schema(description = "Number of unchanged resources", example = "85")
    val unchangedResourcesCount: Int?,
    @Schema(description = "Number of removed resources", example = "5")
    val removedResourcesCount: Int?,
    @Schema(description = "Event counts per phase")
    val phaseEventCounts: PhaseEventCounts?,
)

@Schema(description = "Event counts per harvest phase")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PhaseEventCounts(
    @Schema(description = "Number of INITIATING events", example = "1")
    val initiatingEventsCount: Int?,
    @Schema(description = "Number of HARVESTING events", example = "1")
    val harvestingEventsCount: Int?,
    @Schema(description = "Number of REASONING events", example = "100")
    val reasoningEventsCount: Int?,
    @Schema(description = "Number of RDF_PARSING events", example = "100")
    val rdfParsingEventsCount: Int?,
    @Schema(description = "Number of RESOURCE_PROCESSING events", example = "100")
    val resourceProcessingEventsCount: Int?,
    @Schema(description = "Number of SEARCH_PROCESSING events", example = "100")
    val searchProcessingEventsCount: Int?,
    @Schema(description = "Number of AI_SEARCH_PROCESSING events", example = "100")
    val aiSearchProcessingEventsCount: Int?,
    @Schema(description = "Number of SPARQL_PROCESSING events", example = "100")
    val sparqlProcessingEventsCount: Int?,
)
