package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Marks harvest runs as FAILED when they have received no events within the configured timeout.
 */
@Component
class StaleHarvestRunJob(
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
    @param:Value("\${app.harvest.stale-timeout-minutes:30}") private val staleTimeoutMinutes: Long,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${app.harvest.stale-check-interval-ms:300000}", initialDelay = 60000)
    @Transactional
    fun markStaleRunsAsFailed() {
        try {
            val staleBefore = Instant.now().minus(staleTimeoutMinutes, ChronoUnit.MINUTES)
            val staleRuns = harvestRunRepository.findStaleRuns(staleBefore)

            if (staleRuns.isNotEmpty()) {
                logger.warn("Found ${staleRuns.size} stale harvest run(s) that haven't been updated in $staleTimeoutMinutes minutes")
                staleRuns.forEach { run ->
                    val updatedRun =
                        run.copy(
                            status = "FAILED",
                            errorMessage = "Harvest run timed out - no events received for $staleTimeoutMinutes minutes",
                            runEndedAt = run.updatedAt,
                            updatedAt = Instant.now(),
                        )
                    harvestRunRepository.save(updatedRun)
                    logger.info("Marked stale harvest run ${run.runId} as FAILED (last updated: ${run.updatedAt})")
                    harvestMetricsService.recordRunCompleted(updatedRun, failureReason = "stale_timeout")
                }
            }
        } catch (e: Exception) {
            logger.error("Error marking stale runs as failed", e)
        }
    }
}
