package no.fdk.harvestadmin.converter

import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter
import no.fdk.harvestadmin.model.DataType

@Converter(autoApply = true)
class DataTypeConverter : AttributeConverter<DataType, String> {
    override fun convertToDatabaseColumn(attribute: DataType?): String? = attribute?.value

    override fun convertToEntityAttribute(dbData: String?): DataType? = dbData?.let { DataType.fromString(it) }
}

