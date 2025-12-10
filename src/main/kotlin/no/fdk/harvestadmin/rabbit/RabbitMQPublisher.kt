package no.fdk.harvestadmin.rabbit

import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.HarvestTrigger
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class RabbitMQPublisher(
    private val rabbitTemplate: RabbitTemplate,
    @Value("\${app.rabbitmq.exchange}") private val exchangeName: String,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun publishHarvestTrigger(
        dataType: DataType,
        trigger: HarvestTrigger,
    ) {
        val routingKey =
            when (dataType) {
                DataType.CONCEPT -> "concept.publisher.HarvestTrigger"
                DataType.DATASET -> "dataset.publisher.HarvestTrigger"
                DataType.INFORMATION_MODEL -> "informationmodel.publisher.HarvestTrigger"
                DataType.DATA_SERVICE -> "dataservice.publisher.HarvestTrigger"
                DataType.PUBLIC_SERVICE -> "publicservice.publisher.HarvestTrigger"
                DataType.EVENT -> "event.publisher.HarvestTrigger"
            }

        try {
            rabbitTemplate.convertAndSend(exchangeName, routingKey, trigger)
            logger.info("Published harvest trigger for dataSourceId: ${trigger.dataSourceId}, routingKey: $routingKey")
        } catch (e: Exception) {
            logger.error("Error publishing harvest trigger", e)
            throw e
        }
    }
}
