package no.fdk.harvestadmin.converter

import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter
import no.fdk.harvestadmin.model.DataSourceType

@Converter(autoApply = true)
class DataSourceTypeConverter : AttributeConverter<DataSourceType, String> {
    override fun convertToDatabaseColumn(attribute: DataSourceType?): String? = attribute?.value

    override fun convertToEntityAttribute(dbData: String?): DataSourceType? = dbData?.let { DataSourceType.fromString(it) }
}
