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
    fun countEventsByPhase(@Param("runId") runId: String): List<Array<Any>>

    /**
     * For each of the given phases, counts the resources whose *latest* event (by created_at)
     * is completed (end_time set, no error). Resources are keyed by fdkId, then resourceUri,
     * then row id. Returns rows of [eventType, completedResourceCount] in a single query.
     */
    @Query(
        nativeQuery = true,
        value = """
            SELECT latest.event_type, COUNT(*)
            FROM (
                SELECT DISTINCT ON (event_type, resource_key)
                       event_type, end_time, error_message
                FROM (
                    SELECT event_type, end_time, error_message, created_at,
                        CASE
                            WHEN fdk_id IS NOT NULL THEN 'fdkId:' || fdk_id
                            WHEN resource_uri IS NOT NULL THEN 'resourceUri:' || resource_uri
                            ELSE 'noResource:' || CAST(id AS TEXT)
                        END AS resource_key
                    FROM harvest_events
                    WHERE harvest_run_id = :runId AND event_type IN (:phases)
                ) keyed
                ORDER BY event_type, resource_key, created_at DESC
            ) latest
            WHERE latest.end_time IS NOT NULL AND latest.error_message IS NULL
            GROUP BY latest.event_type
        """,
    )
    fun countCompletedResourcesPerPhase(@Param("runId") runId: String, @Param("phases") phases: Collection<String>): List<Array<Any>>

    /**
     * Counts resources that have at least one event with end_time set (error_message is NOT
     * considered here, matching the processed-resource semantics) in *every* one of the given
     * phases. Resources are keyed by fdkId, then resourceUri, then row id.
     */
    @Query(
        nativeQuery = true,
        value = """
            SELECT COUNT(*) FROM (
                SELECT resource_key
                FROM (
                    SELECT DISTINCT
                        CASE
                            WHEN fdk_id IS NOT NULL THEN 'fdkId:' || fdk_id
                            WHEN resource_uri IS NOT NULL THEN 'resourceUri:' || resource_uri
                            ELSE 'noResource:' || CAST(id AS TEXT)
                        END AS resource_key,
                        event_type
                    FROM harvest_events
                    WHERE harvest_run_id = :runId
                      AND event_type IN (:phases)
                      AND end_time IS NOT NULL
                ) distinct_rp
                GROUP BY resource_key
                HAVING COUNT(*) = :phaseCount
            ) t
        """,
    )
    fun countResourcesCompletedInAllPhases(
        @Param("runId") runId: String,
        @Param("phases") phases: Collection<String>,
        @Param("phaseCount") phaseCount: Long,
    ): Long

    fun findByDataSourceIdOrderByCreatedAtDesc(dataSourceId: String): List<HarvestEventEntity>

    fun findByFdkIdOrderByCreatedAtDesc(fdkId: String): List<HarvestEventEntity>

    fun findByRunIdAndEventTypeAndEndTimeIsNotNull(runId: String, eventType: String): List<HarvestEventEntity>

    fun existsByRunIdAndEventTypeAndFdkId(runId: String, eventType: String, fdkId: String): Boolean

    fun existsByRunIdAndEventTypeAndResourceUri(runId: String, eventType: String, resourceUri: String): Boolean

    fun countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(runId: String, eventType: String): Long
}
