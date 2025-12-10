package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestStatuses(
    val id: String,
    val statuses: List<HarvestStatus>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class HarvestStatus(
    val harvestType: String,
    val status: Status,
    val errorMessage: String? = null,
    val startTime: String,
    val endTime: String? = null,
)

enum class Status(
    val value: String,
) {
    DONE("done"),
    ERROR("error"),
    IN_PROGRESS("in-progress"),
}
