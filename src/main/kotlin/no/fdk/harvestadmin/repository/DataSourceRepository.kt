package no.fdk.harvestadmin.repository

import no.fdk.harvestadmin.entity.DataSourceEntity
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface DataSourceRepository : JpaRepository<DataSourceEntity, String> {
    fun findByPublisherIdIn(publisherIds: List<String>): List<DataSourceEntity>

    @Query(
        """
        SELECT d FROM DataSourceEntity d 
        WHERE (:publisherIds IS NULL OR d.publisherId IN :publisherIds)
        AND (:dataType IS NULL OR d.dataType = :dataType)
        AND (:dataSourceType IS NULL OR d.dataSourceType = :dataSourceType)
    """,
    )
    fun findByFilters(
        @Param("publisherIds") publisherIds: List<String>?,
        @Param("dataType") dataType: DataType?,
        @Param("dataSourceType") dataSourceType: DataSourceType?,
    ): List<DataSourceEntity>

    fun findByUrlAndDataType(
        url: String,
        dataType: DataType,
    ): List<DataSourceEntity>
}
