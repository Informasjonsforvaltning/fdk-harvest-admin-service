package no.fdk.harvestadmin.model

data class HarvestTrigger(
    val runId: String,
    val dataSourceId: String,
    val dataSourceUrl: String,
    val dataType: String,
    val acceptHeader: String,
    val timestamp: Long,
    val publisherId: String,
    val forceUpdate: String = "true",
)
