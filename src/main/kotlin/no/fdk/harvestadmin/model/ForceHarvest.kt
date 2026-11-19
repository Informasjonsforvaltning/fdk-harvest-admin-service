package no.fdk.harvestadmin.model

import io.swagger.v3.oas.annotations.media.Schema

@Schema(description = "Request for forced harvest of specific resources")
data class ForceHarvestRequest(
    @field:Schema(description = "Data type of the resources", example = "dataset")
    val dataType: DataType,
    @field:Schema(description = "FDK identifiers of the resources to update even if unchanged")
    val fdkIds: List<String>,
)

@Schema(description = "Result of a forced harvest request")
data class ForceHarvestResponse(
    @field:Schema(description = "Harvest runs started, one per data source")
    val startedRuns: List<ForcedHarvestRun>,
    @field:Schema(description = "FDK identifiers that could not be matched to an active data source")
    val notFound: List<String>,
)

data class ForcedHarvestRun(val dataSourceId: String, val fdkIds: List<String>)
