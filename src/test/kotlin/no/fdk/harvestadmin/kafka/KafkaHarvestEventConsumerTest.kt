package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.service.HarvestEventProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.verify
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
                .setUnchangedResourcesCount(null)
                .setRemovedResourcesCount(null)
                .build()

        val record = ConsumerRecord<String, HarvestEvent>("harvest-events", 0, 0L, dataSourceId, event)

        // When
        consumer.consumeHarvestEvent(record)

        // Then
        verify(harvestEventProcessor).processEvent(event)
    }
}
