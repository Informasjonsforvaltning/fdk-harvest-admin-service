package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

enum class DataType(
    @JsonValue val value: String,
) {
    CONCEPT("concept"),
    DATASET("dataset"),
    INFORMATION_MODEL("informationmodel"),
    DATA_SERVICE("dataservice"),
    PUBLIC_SERVICE("publicService"),
    EVENT("event"),
    ;

    companion object {
        @JsonCreator
        fun fromString(value: String): DataType? = values().find { it.value.equals(value, ignoreCase = true) }
    }
}
