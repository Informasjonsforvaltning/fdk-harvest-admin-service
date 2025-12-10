package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.UUID
import java.util.concurrent.CompletableFuture

@ExtendWith(MockitoExtension::class)
class KafkaHarvestEventPublisherTest {
    @Mock
    private lateinit var kafkaTemplate: KafkaTemplate<String, HarvestEvent>

    private lateinit var publisher: KafkaHarvestEventPublisher

    private val topicName = "harvest-events"

    @BeforeEach
    fun setUp() {
        publisher = KafkaHarvestEventPublisher(kafkaTemplate, topicName)
    }

    @Test
    fun `should publish harvest event successfully`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(null)
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

        val future = CompletableFuture<SendResult<String, HarvestEvent>>()
        future.complete(null)
        whenever(kafkaTemplate.send(any(), any(), any())).thenReturn(future)

        // When
        publisher.publishEvent(event)

        // Then
        val captor = ArgumentCaptor.forClass(String::class.java)
        val eventCaptor = ArgumentCaptor.forClass(HarvestEvent::class.java)
        verify(kafkaTemplate).send(captor.capture(), captor.capture(), eventCaptor.capture())
        assert(captor.allValues[0] == topicName)
        assert(captor.allValues[1] == dataSourceId)
        assert(eventCaptor.value == event)
    }

    @Test
    fun `should throw exception when Kafka send fails`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(null)
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

        // Mock kafkaTemplate.send() to throw synchronously (not via future)
        whenever(kafkaTemplate.send(any(), any(), any())).thenThrow(RuntimeException("Kafka error"))

        // When/Then
        assertThrows(Exception::class.java) {
            publisher.publishEvent(event)
        }
    }
}
