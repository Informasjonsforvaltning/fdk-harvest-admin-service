package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.model.HarvestCurrentState
import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.repository.DataSourceRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Read queries for harvest runs (listing, details, metrics, current state).
 */
@Service
class HarvestRunQueryService(
    private val harvestRunRepository: HarvestRunRepository,
    private val dataSourceRepository: DataSourceRepository,
    private val completionEvaluator: HarvestCompletionEvaluator,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // Resolves allowed publisher IDs (orgs) to data source IDs for run filtering.
    // Returns null when no restriction (e.g. system admin / API key).
    private fun resolveAllowedDataSourceIds(allowedPublisherIds: List<String>?): List<String>? {
        if (allowedPublisherIds == null) return null
        if (allowedPublisherIds.isEmpty()) return emptyList()
        return dataSourceRepository.findByPublisherIdIn(allowedPublisherIds).map { it.id }
    }

    fun getCurrentState(dataSourceId: String): Pair<List<HarvestCurrentState>, HttpStatus> =
        try {
            val run = harvestRunRepository.findFirstByDataSourceIdOrderByRunStartedAtDesc(dataSourceId)
            val states = run?.let { listOf(it.toHarvestCurrentState()) } ?: emptyList()
            Pair(states, HttpStatus.OK)
        } catch (e: Exception) {
            logger.error("Error getting current state for dataSourceId: $dataSourceId", e)
            Pair(emptyList(), HttpStatus.INTERNAL_SERVER_ERROR)
        }

    fun getAllInProgressStates(): List<HarvestRunDetails> =
        try {
            harvestRunRepository.findAllInProgress().map { run ->
                run.toHarvestRunDetails(completionStatus = completionEvaluator.evaluate(run))
            }
        } catch (e: Exception) {
            logger.error("Error getting all in-progress states", e)
            emptyList()
        }

    fun getPerformanceMetrics(
        dataSourceId: String,
        dataType: String,
        daysBack: Int? = null,
        startDate: Instant? = null,
        endDate: Instant? = null,
        limit: Int? = null,
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> {
        return try {
            if (allowedPublisherIds != null) {
                val dataSource = dataSourceRepository.findById(dataSourceId).orElse(null)
                if (dataSource == null || dataSource.publisherId !in allowedPublisherIds) {
                    return Pair(null, HttpStatus.FORBIDDEN)
                }
            }
            val runs =
                when {
                    limit != null -> {
                        // Get last N completed runs
                        harvestRunRepository.findLastCompletedRuns(dataSourceId, dataType, PageRequest.of(0, limit))
                    }
                    startDate != null && endDate != null -> {
                        // Get runs from date range
                        harvestRunRepository.findCompletedRunsByDateRange(dataSourceId, dataType, startDate, endDate)
                    }
                    startDate != null -> {
                        // Get runs from startDate to now
                        harvestRunRepository.findCompletedRunsByDateRange(dataSourceId, dataType, startDate, Instant.now())
                    }
                    else -> {
                        // Default: use daysBack (backward compatibility)
                        val start = Instant.now().minus((daysBack ?: 30).toLong(), ChronoUnit.DAYS)
                        harvestRunRepository.findCompletedRuns(dataSourceId, dataType, start)
                    }
                }

            if (runs.isEmpty()) {
                Pair(null, HttpStatus.NOT_FOUND)
            } else {
                Pair(buildPerformanceMetrics(runs, dataSourceId, dataType), HttpStatus.OK)
            }
        } catch (e: Exception) {
            logger.error("Error getting performance metrics for dataSourceId: $dataSourceId, dataType: $dataType", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    fun getAllPerformanceMetrics(
        daysBack: Int? = null,
        startDate: Instant? = null,
        endDate: Instant? = null,
        limit: Int? = null,
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestPerformanceMetrics?, HttpStatus> {
        return try {
            val allowedDataSourceIds = resolveAllowedDataSourceIds(allowedPublisherIds)
            if (allowedPublisherIds != null && allowedDataSourceIds != null && allowedDataSourceIds.isEmpty()) {
                return Pair(null, HttpStatus.NOT_FOUND)
            }
            val runs =
                when {
                    limit != null -> {
                        // Get last N completed runs across all data sources
                        harvestRunRepository.findLastAllCompletedRuns(allowedDataSourceIds, PageRequest.of(0, limit))
                    }
                    startDate != null && endDate != null -> {
                        // Get runs from date range across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, endDate, allowedDataSourceIds)
                    }
                    startDate != null -> {
                        // Get runs from startDate to now across all data sources
                        harvestRunRepository.findAllCompletedRunsByDateRange(startDate, Instant.now(), allowedDataSourceIds)
                    }
                    else -> {
                        // Default: use daysBack (backward compatibility)
                        val start = Instant.now().minus((daysBack ?: 30).toLong(), ChronoUnit.DAYS)
                        harvestRunRepository.findAllCompletedRuns(start, allowedDataSourceIds)
                    }
                }

            if (runs.isEmpty()) {
                Pair(null, HttpStatus.NOT_FOUND)
            } else {
                Pair(buildPerformanceMetrics(runs, dataSourceId = null, dataType = null), HttpStatus.OK)
            }
        } catch (e: Exception) {
            logger.error("Error getting all performance metrics", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    fun getHarvestRun(
        runId: String,
        allowedPublisherIds: List<String>? = null,
    ): Pair<HarvestRunDetails?, HttpStatus> {
        return try {
            val run = harvestRunRepository.findByRunId(runId) ?: return Pair(null, HttpStatus.NOT_FOUND)
            if (allowedPublisherIds != null) {
                val dataSource = dataSourceRepository.findById(run.dataSourceId).orElse(null)
                if (dataSource == null || dataSource.publisherId !in allowedPublisherIds) {
                    return Pair(null, HttpStatus.FORBIDDEN)
                }
            }
            Pair(
                run.toHarvestRunDetails(completionStatus = completionEvaluator.evaluate(run)),
                HttpStatus.OK,
            )
        } catch (e: Exception) {
            logger.error("Error getting harvest run for runId: $runId", e)
            Pair(null, HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    fun getHarvestRuns(
        dataSourceId: String? = null,
        dataType: String? = null,
        status: String? = null,
        offset: Int = 0,
        limit: Int = 50,
        allowedPublisherIds: List<String>? = null,
    ): Pair<List<HarvestRunDetails>, Long> {
        return try {
            val allowedDataSourceIds = resolveAllowedDataSourceIds(allowedPublisherIds)
            if (allowedPublisherIds != null && allowedDataSourceIds != null && allowedDataSourceIds.isEmpty()) {
                return Pair(emptyList(), 0L)
            }
            // Calculate page number from offset (Spring Data uses 0-indexed page numbers)
            val page = if (limit > 0) offset / limit else 0
            val pageable = PageRequest.of(page, limit)
            val runs = harvestRunRepository.findRunsWithFilters(dataSourceId, dataType, status, allowedDataSourceIds, pageable)
            val totalCount = harvestRunRepository.countRunsWithFilters(dataSourceId, dataType, status, allowedDataSourceIds)

            val runDetails =
                runs.map { run ->
                    run.toHarvestRunDetails(
                        completionStatus = if (run.status == "COMPLETED") null else completionEvaluator.evaluate(run),
                    )
                }
            Pair(runDetails, totalCount)
        } catch (e: Exception) {
            logger.error("Error getting harvest runs for dataSourceId: $dataSourceId, dataType: $dataType, status: $status", e)
            Pair(emptyList(), 0L)
        }
    }
}
