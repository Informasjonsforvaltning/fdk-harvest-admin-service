package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.swagger.v3.oas.annotations.media.Schema

@Schema(description = "Request for system admin resource removal (publishes REMOVING events to Kafka)")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class RemoveResourcesRequest(
    @field:Schema(
        description = "Data type for the resources being removed (required by the HarvestEvent schema)",
        example = "dataset",
        allowableValues = ["concept", "dataset", "informationmodel", "dataservice", "publicService", "event"],
    )
    val dataType: DataType,
    @field:Schema(description = "Resources to remove (one Kafka message per item)")
    val resources: List<ResourceToRemove>,
)

@Schema(description = "Single resource identifier to remove")
@JsonInclude(JsonInclude.Include.NON_NULL)
data class ResourceToRemove(
    @field:Schema(description = "FDK identifier for the resource", example = "fdk-id-123")
    val fdkId: String,
    @field:Schema(description = "URI of the resource", example = "https://example.com/resource/123")
    val resourceUri: String,
)
