package no.fdk.harvestadmin.rabbit

import no.fdk.harvestadmin.model.DataSource
import no.fdk.harvestadmin.service.DataSourceService
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.amqp.support.AmqpHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class RabbitMQListener(
    private val dataSourceService: DataSourceService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @RabbitListener(queues = ["\${app.rabbitmq.queue}"])
    fun handleMessage(
        @Payload body: Any,
        @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) routingKey: String,
    ) {
        logger.info("Received message with routing key: $routingKey")

        when {
            routingKey.contains("NewDataSource") -> {
                handleNewDataSource(body)
            }
            else -> {
                logger.warn("Unknown routing key: $routingKey")
            }
        }
    }

    private fun handleNewDataSource(body: Any) {
        try {
            val dataSource =
                when (body) {
                    is DataSource -> body
                    is Map<*, *> -> {
                        // Convert map to DataSource
                        val map = body as Map<String, Any>
                        DataSource(
                            id = map["id"] as? String,
                            dataSourceType =
                                no.fdk.harvestadmin.model.DataSourceType.fromString(
                                    map["dataSourceType"] as? String ?: "",
                                ) ?: throw IllegalArgumentException("Invalid dataSourceType"),
                            dataType =
                                no.fdk.harvestadmin.model.DataType.fromString(
                                    map["dataType"] as? String ?: "",
                                ) ?: throw IllegalArgumentException("Invalid dataType"),
                            url = map["url"] as? String ?: throw IllegalArgumentException("url is required"),
                            acceptHeader = map["acceptHeader"] as? String,
                            publisherId =
                                map["publisherId"] as? String
                                    ?: throw IllegalArgumentException("publisherId is required"),
                            description = map["description"] as? String,
                        )
                    }
                    else -> throw IllegalArgumentException("Unknown message type: ${body::class.simpleName}")
                }

            dataSourceService.createDataSourceFromRabbitMessage(dataSource)
            logger.info("Data source created from RabbitMQ message")
        } catch (e: Exception) {
            logger.error("Error processing NewDataSource message", e)
        }
    }
}
