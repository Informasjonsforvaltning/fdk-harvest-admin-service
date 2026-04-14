package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.service.HarvestEventProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaHarvestEventConsumer(
    private val harvestEventProcessor: HarvestEventProcessor,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["\${app.kafka.topic.harvest-events}"],
        groupId = "\${spring.kafka.consumer.group-id:fdk-harvest-admin-service}",
        containerFactory = "kafkaListenerContainerFactory",
        id = HARVEST_LISTENER_ID,
    )
    fun consumeHarvestEvent(record: ConsumerRecord<String, HarvestEvent>) {
        logger.debug("Received harvest event - offset: ${record.offset()}, partition: ${record.partition()}")

        val event = record.value()
        harvestEventProcessor.processEvent(event)
    }

    companion object {
        const val HARVEST_LISTENER_ID = "harvest-event-listener"
    }
}
