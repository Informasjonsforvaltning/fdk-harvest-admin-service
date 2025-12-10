package no.fdk.harvestadmin.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

enum class DataSourceType(
    @JsonValue val value: String,
) {
    SKOS_AP_NO("SKOS-AP-NO"),
    DCAT_AP_NO("DCAT-AP-NO"),
    CPSV_AP_NO("CPSV-AP-NO"),
    TBX("TBX"),
    MODELL_DCAT_AP_NO("ModellDCAT-AP-NO"),
    ;

    companion object {
        @JsonCreator
        fun fromString(value: String): DataSourceType? {
            // Only match by enum value (e.g., "DCAT-AP-NO") to match database constraint
            // The database constraint expects hyphenated format, not enum names
            return values().find { it.value.equals(value, ignoreCase = true) }
        }
    }
}
