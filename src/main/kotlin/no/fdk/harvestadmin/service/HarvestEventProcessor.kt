package no.fdk.harvestadmin.service

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.harvest.HarvestEvent
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
            // Skip events published by this service as commands/triggers
            when (event.phase) {
                no.fdk.harvest.HarvestPhase.INITIATING -> {
                    logger.debug("Skipping INITIATING event (published by this service)")
                    return
                }

                no.fdk.harvest.HarvestPhase.REMOVING -> {
                    logger.debug("Skipping REMOVING event (published by this service)")
                    return
                }

                else -> {
                    harvestRunService.persistEvent(event)
                    harvestMetricsService.recordEventProcessed(event)
                    logger.debug("Successfully processed harvest event: phase={}, runId={}", event.phase, event.runId)
                }
            }
        } catch (e: Exception) {
            logger.error("Error processing harvest event: phase=${event.phase}, runId=${event.runId}", e)
            throw e
        }
    }
}
