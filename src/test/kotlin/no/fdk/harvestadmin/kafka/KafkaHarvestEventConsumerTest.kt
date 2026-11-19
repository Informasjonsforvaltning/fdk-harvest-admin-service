package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.service.HarvestEventBatchException
import no.fdk.harvestadmin.service.HarvestEventProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.listener.BatchListenerFailedException
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class KafkaHarvestEventConsumerTest {
    @Mock
    private lateinit var harvestEventProcessor: HarvestEventProcessor

    private lateinit var consumer: KafkaHarvestEventConsumer

    @BeforeEach
    fun setUp() {
        consumer = KafkaHarvestEventConsumer(harvestEventProcessor)
    }

    @Test
    fun `should process harvest event from Kafka`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(runId)
                .setDataSourceId(dataSourceId)
                .setDataType(no.fdk.harvest.DataType.dataset)
                .setStartTime(null)
                .setEndTime(null)
                .setErrorMessage(null)
                .setFdkId(null)
                .setResourceUri(null)
                .setChangedResourcesCount(null)
                .setRemovedResourcesCount(null)
                .build()

        val record = ConsumerRecord<String, HarvestEvent>("harvest-events", 0, 0L, dataSourceId, event)

        // When
        consumer.consumeHarvestEvents(listOf(record))

        // Then
        verify(harvestEventProcessor).processEvents(listOf(event))
    }

    @Test
    fun `should report the failed record to the error handler`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val events =
            (0..2).map {
                HarvestEvent
                    .newBuilder()
                    .setPhase(HarvestPhase.REASONING)
                    .setRunId(UUID.randomUUID().toString())
                    .setDataSourceId(dataSourceId)
                    .setDataType(no.fdk.harvest.DataType.dataset)
                    .build()
            }
        val records = events.mapIndexed { i, event -> ConsumerRecord("harvest-events", 0, 10L + i, dataSourceId, event) }
        whenever(harvestEventProcessor.processEvents(events)).thenThrow(HarvestEventBatchException(1, RuntimeException("boom")))

        // When
        val exception = assertThrows<BatchListenerFailedException> { consumer.consumeHarvestEvents(records) }

        // Then
        assertSame(records[1], exception.record)
    }
}
