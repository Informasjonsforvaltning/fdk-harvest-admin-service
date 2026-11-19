package no.fdk.harvestadmin.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.service.HarvestEventBatchException
import no.fdk.harvestadmin.service.HarvestEventProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.BatchListenerFailedException
import org.springframework.stereotype.Component

@Component
class KafkaHarvestEventConsumer(private val harvestEventProcessor: HarvestEventProcessor) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["\${app.kafka.topic.harvest-events}"],
        groupId = "\${spring.kafka.consumer.group-id:fdk-harvest-admin-service}",
        containerFactory = "kafkaListenerContainerFactory",
        id = HARVEST_LISTENER_ID,
    )
    fun consumeHarvestEvents(records: List<ConsumerRecord<String, HarvestEvent>>) {
        logger.debug("Received batch of {} harvest event(s)", records.size)

        try {
            harvestEventProcessor.processEvents(records.map { it.value() })
        } catch (e: HarvestEventBatchException) {
            // Lets the error handler commit the records before the failed one and retry from it
            val failedRecord = records[e.failedIndex]
            throw BatchListenerFailedException(
                "Failed to process harvest event - offset: ${failedRecord.offset()}, partition: ${failedRecord.partition()}",
                e.cause,
                failedRecord,
            )
        }
    }

    companion object {
        const val HARVEST_LISTENER_ID = "harvest-event-listener"
    }
}
