package no.fdk.harvestadmin.kafka

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.whenever
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer

@ExtendWith(MockitoExtension::class)
class KafkaManagerTest {
    @Mock
    private lateinit var registry: KafkaListenerEndpointRegistry

    @Mock
    private lateinit var container: MessageListenerContainer

    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var kafkaManager: KafkaManager

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        kafkaManager = KafkaManager(registry, meterRegistry)
        kafkaManager.registerPausedGauges()
    }

    @Test
    fun `pause and resume update harvest kafka listener paused gauge`() {
        val listenerId = KafkaHarvestEventConsumer.HARVEST_LISTENER_ID
        whenever(registry.listenerContainers).thenReturn(listOf(container))
        whenever(container.listenerId).thenReturn(listenerId)

        assertEquals(0.0, pausedGaugeValue(listenerId))

        kafkaManager.pause(listenerId)
        assertEquals(1.0, pausedGaugeValue(listenerId))

        kafkaManager.resume(listenerId)
        assertEquals(0.0, pausedGaugeValue(listenerId))
    }

    private fun pausedGaugeValue(listenerId: String): Double =
        meterRegistry
            .find("harvest.kafka.listener.paused")
            .tag("listener_id", listenerId)
            .gauge()!!
            .value()
}
