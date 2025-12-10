package no.fdk.harvestadmin.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Index
import jakarta.persistence.Table
import no.fdk.harvestadmin.model.DataSource
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType

@Entity
@Table(
    name = "data_sources",
    indexes = [
        Index(name = "idx_publisher_id", columnList = "publisher_id"),
        Index(name = "idx_url_data_type", columnList = "url,data_type"),
    ],
)
data class DataSourceEntity(
    @Id
    @Column(name = "id", length = 36)
    var id: String,
    @Column(name = "data_source_type", nullable = false, length = 50)
    var dataSourceType: DataSourceType,
    @Column(name = "data_type", nullable = false, length = 50)
    var dataType: DataType,
    @Column(name = "url", nullable = false, length = 2048)
    var url: String,
    @Column(name = "accept_header", length = 255)
    var acceptHeader: String? = null,
    @Column(name = "publisher_id", nullable = false, length = 255)
    var publisherId: String,
    @Column(name = "description", columnDefinition = "TEXT")
    var description: String? = null,
) {
    fun toModel(): DataSource =
        DataSource(
            id = id,
            dataSourceType = dataSourceType,
            dataType = dataType,
            url = url,
            acceptHeader = acceptHeader,
            publisherId = publisherId,
            description = description,
        )

    fun updateFromModel(dataSource: DataSource) {
        // Note: id and publisherId are not updated - id is immutable, publisherId should not change
        this.dataSourceType = dataSource.dataSourceType
        this.dataType = dataSource.dataType
        this.url = dataSource.url
        this.acceptHeader = dataSource.acceptHeader
        this.description = dataSource.description
    }

    companion object {
        fun fromModel(dataSource: DataSource): DataSourceEntity =
            DataSourceEntity(
                id = dataSource.id ?: "",
                dataSourceType = dataSource.dataSourceType,
                dataType = dataSource.dataType,
                url = dataSource.url,
                acceptHeader = dataSource.acceptHeader,
                publisherId = dataSource.publisherId,
                description = dataSource.description,
            )
    }
}
