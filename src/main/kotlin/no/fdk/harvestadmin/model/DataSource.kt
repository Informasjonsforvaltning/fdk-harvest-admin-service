package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Pattern

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DataSource(
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    val id: String? = null,
    @field:NotNull(message = "dataSourceType is required")
    val dataSourceType: DataSourceType,
    @field:NotNull(message = "dataType is required")
    val dataType: DataType,
    @field:NotBlank(message = "url is required")
    @field:Pattern(
        regexp = "^https?://.+",
        message = "Invalid URL format",
    )
    val url: String,
    @JsonProperty("acceptHeaderValue")
    val acceptHeader: String? = null,
    @field:NotBlank(message = "publisherId is required")
    @field:Pattern(
        regexp = "^[a-zA-Z0-9-_]+$",
        message = "publisherId must contain only alphanumeric characters, hyphens, and underscores",
    )
    val publisherId: String,
    val description: String? = null,
)
