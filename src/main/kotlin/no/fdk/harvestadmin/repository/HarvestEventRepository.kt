package no.fdk.harvestadmin.repository

import no.fdk.harvestadmin.entity.HarvestEventEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface HarvestEventRepository : JpaRepository<HarvestEventEntity, Long> {
    fun findByDataSourceIdOrderByTimestampDesc(dataSourceId: String): List<HarvestEventEntity>

    fun findByFdkIdOrderByTimestampDesc(fdkId: String): List<HarvestEventEntity>

    fun findByRunIdAndEventTypeAndEndTimeIsNotNull(
        runId: String,
        eventType: String,
    ): List<HarvestEventEntity>

    fun existsByRunIdAndEventTypeAndFdkId(
        runId: String,
        eventType: String,
        fdkId: String,
    ): Boolean

    fun existsByRunIdAndEventTypeAndResourceUri(
        runId: String,
        eventType: String,
        resourceUri: String,
    ): Boolean

    fun findByRunIdAndFdkId(
        runId: String,
        fdkId: String,
    ): List<HarvestEventEntity>

    fun findByRunIdAndResourceUri(
        runId: String,
        resourceUri: String,
    ): List<HarvestEventEntity>
}
