package no.fdk.harvestadmin.repository

import no.fdk.harvestadmin.entity.HarvestEventEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface HarvestEventRepository : JpaRepository<HarvestEventEntity, Long> {
    /** Counts events per phase for a run in a single query, returning rows of [eventType, count]. */
    @Query(
        "SELECT e.eventType, COUNT(e) FROM HarvestEventEntity e WHERE e.runId = :runId GROUP BY e.eventType",
    )
    fun countEventsByPhase(
        @Param("runId") runId: String,
    ): List<Array<Any>>

    fun findByDataSourceIdOrderByCreatedAtDesc(dataSourceId: String): List<HarvestEventEntity>

    fun findByFdkIdOrderByCreatedAtDesc(fdkId: String): List<HarvestEventEntity>

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

    fun countByRunIdAndEventType(
        runId: String,
        eventType: String,
    ): Long

    fun findByRunIdAndEventType(
        runId: String,
        eventType: String,
    ): List<HarvestEventEntity>

    fun countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
        runId: String,
        eventType: String,
    ): Long
}
