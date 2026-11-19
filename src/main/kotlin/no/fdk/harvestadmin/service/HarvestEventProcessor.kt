package no.fdk.harvestadmin.service

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service

class HarvestEventBatchException(val failedIndex: Int, cause: Throwable) :
    RuntimeException("Failed to process harvest event at batch index $failedIndex", cause)

@Service
class HarvestEventProcessor(
    private val harvestEventIngestionService: HarvestEventIngestionService,
    private val harvestMetricsService: HarvestMetricsService,
    @param:Qualifier("harvestEventCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processEvents(events: List<HarvestEvent>) {
        val toIngest = events.withIndex().filter { (_, event) -> event.phase !in SKIPPED_PHASES }
        if (toIngest.size < events.size) {
            logger.debug("Skipping {} INITIATING/REMOVING event(s) (published by this service)", events.size - toIngest.size)
        }
        if (toIngest.isEmpty()) {
            return
        }

        circuitBreaker.executeRunnable {
            try {
                harvestEventIngestionService.persistEvents(toIngest.map { it.value })
            } catch (e: Exception) {
                logger.warn("Failed to process batch of ${toIngest.size} harvest event(s), retrying one at a time", e)
                processIndividually(toIngest)
                return@executeRunnable
            }
            toIngest.forEach { (_, event) -> harvestMetricsService.recordEventProcessed(event) }
            logger.debug("Successfully processed batch of {} harvest event(s)", toIngest.size)
        }
    }

    private fun processIndividually(events: List<IndexedValue<HarvestEvent>>) {
        events.forEach { (index, event) ->
            try {
                harvestEventIngestionService.persistEvent(event)
                harvestMetricsService.recordEventProcessed(event)
            } catch (e: Exception) {
                logger.error("Error processing harvest event: phase=${event.phase}, runId=${event.runId}", e)
                harvestMetricsService.recordEventProcessingFailed(event.phase.name, event.dataType.name)
                throw HarvestEventBatchException(index, e)
            }
        }
    }

    companion object {
        private val SKIPPED_PHASES = setOf(HarvestPhase.INITIATING, HarvestPhase.REMOVING)
    }
}
