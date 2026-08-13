package no.fdk.harvestadmin.kafka

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
class KafkaManager(private val registry: KafkaListenerEndpointRegistry, private val meterRegistry: MeterRegistry) {
    private val pausedByListenerId = ConcurrentHashMap<String, AtomicInteger>()

    @PostConstruct
    fun registerPausedGauges() {
        registerPausedGauge(KafkaHarvestEventConsumer.HARVEST_LISTENER_ID)
    }

    fun pause(id: String) {
        LOGGER.debug("Pausing kafka listener containers with id: {}", id)
        registry.listenerContainers
            .filter { it.listenerId.equals(id) }
            .forEach { it.pause() }
        pausedState(id).set(1)
    }

    fun resume(id: String) {
        LOGGER.debug("Resuming kafka listener containers with id: {}", id)
        registry.listenerContainers
            .filter { it.listenerId.equals(id) }
            .forEach { it.resume() }
        pausedState(id).set(0)
    }

    private fun pausedState(listenerId: String): AtomicInteger {
        registerPausedGauge(listenerId)
        return pausedByListenerId.getValue(listenerId)
    }

    private fun registerPausedGauge(listenerId: String) {
        pausedByListenerId.computeIfAbsent(listenerId) { id ->
            val state = AtomicInteger(0)
            Gauge
                .builder("harvest.kafka.listener.paused") { state.get().toDouble() }
                .description("Whether the Kafka listener is paused (1) or not (0)")
                .tag("listener_id", id)
                .register(meterRegistry)
            state
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaManager::class.java)
    }
}
