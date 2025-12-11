package no.fdk.harvestadmin.repository

import no.fdk.harvestadmin.entity.HarvestRunEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.Instant

@Repository
interface HarvestRunRepository : JpaRepository<HarvestRunEntity, Long> {
    fun findByDataSourceIdOrderByRunStartedAtDesc(dataSourceId: String): List<HarvestRunEntity>

    fun findByDataSourceIdAndDataTypeOrderByRunStartedAtDesc(
        dataSourceId: String,
        dataType: String,
    ): List<HarvestRunEntity>

    // Find run by runId (UUID)
    fun findByRunId(runId: String): HarvestRunEntity?

    // Find current state (most recent IN_PROGRESS run)
    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'IN_PROGRESS' ORDER BY h.runStartedAt DESC",
    )
    fun findCurrentRun(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
    ): HarvestRunEntity?

    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.status = 'IN_PROGRESS' ORDER BY h.runStartedAt DESC",
    )
    fun findCurrentRunsByDataSourceId(dataSourceId: String): List<HarvestRunEntity>

    @Query("SELECT h FROM HarvestRunEntity h WHERE h.status = 'IN_PROGRESS' ORDER BY h.updatedAt DESC")
    fun findAllInProgress(): List<HarvestRunEntity>

    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'COMPLETED' AND h.runStartedAt >= :startDate ORDER BY h.runStartedAt DESC",
    )
    fun findCompletedRuns(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
        @Param("startDate") startDate: Instant,
    ): List<HarvestRunEntity>

    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'COMPLETED' AND h.runStartedAt >= :startDate AND h.runStartedAt <= :endDate ORDER BY h.runStartedAt DESC",
    )
    fun findCompletedRunsByDateRange(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
        @Param("startDate") startDate: Instant,
        @Param("endDate") endDate: Instant,
    ): List<HarvestRunEntity>

    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'COMPLETED' ORDER BY h.runStartedAt DESC",
    )
    fun findLastCompletedRuns(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
        pageable: Pageable,
    ): List<HarvestRunEntity>

    @Query(
        "SELECT AVG(h.totalDurationMs) FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'COMPLETED' AND h.runStartedAt >= :startDate",
    )
    fun getAverageTotalDuration(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
        @Param("startDate") startDate: Instant,
    ): Double?

    @Query(
        "SELECT AVG(h.harvestDurationMs) FROM HarvestRunEntity h WHERE h.dataSourceId = :dataSourceId AND h.dataType = :dataType AND h.status = 'COMPLETED' AND h.runStartedAt >= :startDate",
    )
    fun getAverageHarvestDuration(
        @Param("dataSourceId") dataSourceId: String,
        @Param("dataType") dataType: String,
        @Param("startDate") startDate: Instant,
    ): Double?

    // Global metrics queries (all data sources)
    @Query("SELECT h FROM HarvestRunEntity h WHERE h.status = 'COMPLETED' AND h.runStartedAt >= :startDate ORDER BY h.runStartedAt DESC")
    fun findAllCompletedRuns(
        @Param("startDate") startDate: Instant,
    ): List<HarvestRunEntity>

    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.status = 'COMPLETED' AND h.runStartedAt >= :startDate AND h.runStartedAt <= :endDate ORDER BY h.runStartedAt DESC",
    )
    fun findAllCompletedRunsByDateRange(
        @Param("startDate") startDate: Instant,
        @Param("endDate") endDate: Instant,
    ): List<HarvestRunEntity>

    @Query("SELECT h FROM HarvestRunEntity h WHERE h.status = 'COMPLETED' ORDER BY h.runStartedAt DESC")
    fun findLastAllCompletedRuns(pageable: Pageable): List<HarvestRunEntity>

    // Find stale runs (IN_PROGRESS runs that haven't been updated recently)
    @Query(
        "SELECT h FROM HarvestRunEntity h WHERE h.status = 'IN_PROGRESS' AND h.updatedAt < :staleBefore",
    )
    fun findStaleRuns(
        @Param("staleBefore") staleBefore: Instant,
    ): List<HarvestRunEntity>
}
