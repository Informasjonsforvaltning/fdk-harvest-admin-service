package no.fdk.harvestadmin.service

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.repository.HarvestRunRepository
import no.fdk.harvestadmin.service.HarvestRunService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class HarvestEventProcessor(
    private val harvestRunService: HarvestRunService,
    private val harvestMetricsService: HarvestMetricsService,
    private val harvestRunRepository: HarvestRunRepository,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @CircuitBreaker(name = "harvestEventConsumer")
    fun processEvent(event: HarvestEvent) {
        try {
            // Skip trigger events since we persist them directly in DataSourceService
            if (event.phase == no.fdk.harvest.HarvestPhase.INITIATING) {
                logger.debug("Skipping INITIATING event (persisted directly)")
                return
            }

            // Check run status before processing to avoid recording metrics for completed runs
            val runId = event.runId?.toString()
            val currentRun = runId?.let { harvestRunRepository.findByRunId(it) }
            val isRunInProgress = currentRun?.status == "IN_PROGRESS"

            harvestRunService.persistEvent(event)

            // Only record event metrics if the run is still IN_PROGRESS
            // This prevents recording metrics for late-arriving events after a run has completed
            if (isRunInProgress) {
                harvestMetricsService.recordEventProcessed(event)
            }

            logger.debug("Successfully processed harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}")
        } catch (e: Exception) {
            logger.error("Error processing harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}", e)
            throw e
        }
    }
}
