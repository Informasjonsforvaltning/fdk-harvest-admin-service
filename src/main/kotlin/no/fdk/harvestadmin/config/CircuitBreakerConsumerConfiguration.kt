package no.fdk.harvestadmin.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker.StateTransition
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import no.fdk.harvestadmin.kafka.KafkaHarvestEventConsumer
import no.fdk.harvestadmin.kafka.KafkaManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
open class CircuitBreakerConsumerConfiguration(
    private val kafkaManager: KafkaManager,
) {
    @Bean
    open fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val defaultConfig =
            CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(60f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build()

        val registry = CircuitBreakerRegistry.of(defaultConfig)
        registerListeners(registry)
        return registry
    }

    open fun registerListeners(registry: CircuitBreakerRegistry) {
        attachListener(
            registry,
            HARVEST_EVENT_CIRCUIT_BREAKER_ID,
            KafkaHarvestEventConsumer.HARVEST_LISTENER_ID,
        )
    }

    private fun attachListener(
        registry: CircuitBreakerRegistry,
        breakerId: String,
        listenerId: String,
    ) {
        registry
            .circuitBreaker(breakerId)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event, listenerId)
            }
    }

    private fun handleStateTransition(
        event: CircuitBreakerOnStateTransitionEvent,
        listenerId: String,
    ) {
        LOGGER.debug("Handling state transition in circuit breaker {}", event)
        when (event.stateTransition) {
            StateTransition.CLOSED_TO_OPEN,
            StateTransition.CLOSED_TO_FORCED_OPEN,
            StateTransition.HALF_OPEN_TO_OPEN,
            -> {
                LOGGER.warn("Circuit breaker opened, pausing Kafka listener: {}", listenerId)
                kafkaManager.pause(listenerId)
            }

            StateTransition.OPEN_TO_HALF_OPEN,
            StateTransition.HALF_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_HALF_OPEN,
            -> {
                LOGGER.info("Circuit breaker closed, resuming Kafka listener: {}", listenerId)
                kafkaManager.resume(listenerId)
            }

            else -> throw IllegalStateException("Unknown transition state: " + event.stateTransition)
        }
    }

    @Bean
    open fun harvestEventCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(HARVEST_EVENT_CIRCUIT_BREAKER_ID)

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(CircuitBreakerConsumerConfiguration::class.java)
        const val HARVEST_EVENT_CIRCUIT_BREAKER_ID = "harvest-event-consumer-cb"
    }
}
