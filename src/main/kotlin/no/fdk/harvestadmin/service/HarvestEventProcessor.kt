package no.fdk.harvestadmin.service

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.service.HarvestRunService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class HarvestEventProcessor(
    private val harvestRunService: HarvestRunService,
    private val harvestMetricsService: HarvestMetricsService,
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

            harvestRunService.persistEvent(event)

            // Record metrics
            harvestMetricsService.recordEventProcessed(event)

            logger.debug("Successfully processed harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}")
        } catch (e: Exception) {
            logger.error("Error processing harvest event: phase=${event.phase}, dataSourceId=${event.dataSourceId}", e)
            throw e
        }
    }
}
