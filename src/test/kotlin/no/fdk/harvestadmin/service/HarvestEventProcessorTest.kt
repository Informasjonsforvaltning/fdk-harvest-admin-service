package no.fdk.harvestadmin.service

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class HarvestEventProcessorTest {
    @Mock
    private lateinit var ingestionService: HarvestEventIngestionService

    @Mock
    private lateinit var metricsService: HarvestMetricsService

    private lateinit var processor: HarvestEventProcessor

    @BeforeEach
    fun setUp() {
        processor = HarvestEventProcessor(ingestionService, metricsService, CircuitBreaker.ofDefaults("test"))
    }

    private fun event(phase: HarvestPhase): HarvestEvent = HarvestEvent
        .newBuilder()
        .setPhase(phase)
        .setRunId(UUID.randomUUID().toString())
        .setDataSourceId(UUID.randomUUID().toString())
        .setDataType(DataType.dataset)
        .build()

    @Test
    fun `should persist the whole batch in one call and skip events published by this service`() {
        val initiating = event(HarvestPhase.INITIATING)
        val harvesting = event(HarvestPhase.HARVESTING)
        val removing = event(HarvestPhase.REMOVING)
        val reasoning = event(HarvestPhase.REASONING)

        processor.processEvents(listOf(initiating, harvesting, removing, reasoning))

        verify(ingestionService).persistEvents(listOf(harvesting, reasoning))
        verify(ingestionService, never()).persistEvent(any())
        verify(metricsService).recordEventProcessed(harvesting)
        verify(metricsService).recordEventProcessed(reasoning)
    }

    @Test
    fun `should not call ingestion when the batch only has skipped events`() {
        processor.processEvents(listOf(event(HarvestPhase.INITIATING), event(HarvestPhase.REMOVING)))

        verifyNoInteractions(ingestionService)
    }

    @Test
    fun `should retry one at a time and report the index of the failing event`() {
        val initiating = event(HarvestPhase.INITIATING)
        val good = event(HarvestPhase.HARVESTING)
        val bad = event(HarvestPhase.REASONING)
        val after = event(HarvestPhase.RDF_PARSING)
        whenever(ingestionService.persistEvents(listOf(good, bad, after))).thenThrow(RuntimeException("batch failed"))
        doAnswer { if (it.arguments[0] == bad) throw RuntimeException("bad event") }
            .whenever(ingestionService)
            .persistEvent(any())

        val exception =
            assertThrows<HarvestEventBatchException> {
                processor.processEvents(listOf(initiating, good, bad, after))
            }

        // Index refers to the original batch, including skipped events
        assertEquals(2, exception.failedIndex)
        verify(ingestionService).persistEvent(good)
        verify(ingestionService, never()).persistEvent(after)
        verify(metricsService).recordEventProcessed(good)
        verify(metricsService).recordEventProcessingFailed("REASONING", "dataset")
    }

    @Test
    fun `should succeed when retrying one at a time succeeds`() {
        val first = event(HarvestPhase.HARVESTING)
        val second = event(HarvestPhase.REASONING)
        whenever(ingestionService.persistEvents(listOf(first, second))).thenThrow(RuntimeException("transient"))

        processor.processEvents(listOf(first, second))

        verify(ingestionService).persistEvent(first)
        verify(ingestionService).persistEvent(second)
        verify(metricsService).recordEventProcessed(first)
        verify(metricsService).recordEventProcessed(second)
    }
}
