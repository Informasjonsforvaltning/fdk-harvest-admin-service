package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaHarvestEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, HarvestEvent>,
    @Value("\${app.kafka.topic.harvest-events}") private val topicName: String,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun publishEvent(event: HarvestEvent) {
        try {
            val key = event.dataSourceId.toString()
            kafkaTemplate.send(topicName, key, event)
            logger.info("Published harvest event to Kafka - phase: ${event.phase}, dataSourceId: ${event.dataSourceId}, topic: $topicName")
        } catch (e: Exception) {
            logger.error("Error publishing harvest event to Kafka", e)
            throw e
        }
    }
}
