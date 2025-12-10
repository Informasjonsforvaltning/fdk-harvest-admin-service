package no.fdk.harvestadmin.config

import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.ExchangeBuilder
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.QueueBuilder
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class RabbitMQConfig(
    @Value("\${app.rabbitmq.exchange}") private val exchangeName: String,
    @Value("\${app.rabbitmq.exchange-type}") private val exchangeType: String,
    @Value("\${app.rabbitmq.queue}") private val queueName: String,
    @Value("\${app.rabbitmq.routing-keys.new-datasource}") private val newDataSourceKey: String,
    @Value("\${app.rabbitmq.routing-keys.harvested}") private val harvestedKey: String,
    @Value("\${app.rabbitmq.routing-keys.ingested}") private val ingestedKey: String,
    @Value("\${app.rabbitmq.routing-keys.reasoned}") private val reasonedKey: String,
) {
    @Bean
    fun topicExchange(): TopicExchange =
        ExchangeBuilder
            .topicExchange(exchangeName)
            .durable(false)
            .build()

    @Bean
    fun queue(): Queue =
        QueueBuilder
            .durable(queueName)
            .build()

    @Bean
    fun newDataSourceBinding(
        topicExchange: TopicExchange,
        queue: Queue,
    ): Binding =
        BindingBuilder
            .bind(queue)
            .to(topicExchange)
            .with(newDataSourceKey)

    @Bean
    fun harvestedBinding(
        topicExchange: TopicExchange,
        queue: Queue,
    ): Binding =
        BindingBuilder
            .bind(queue)
            .to(topicExchange)
            .with(harvestedKey)

    @Bean
    fun ingestedBinding(
        topicExchange: TopicExchange,
        queue: Queue,
    ): Binding =
        BindingBuilder
            .bind(queue)
            .to(topicExchange)
            .with(ingestedKey)

    @Bean
    fun reasonedBinding(
        topicExchange: TopicExchange,
        queue: Queue,
    ): Binding =
        BindingBuilder
            .bind(queue)
            .to(topicExchange)
            .with(reasonedKey)

    @Bean
    fun messageConverter(): Jackson2JsonMessageConverter = Jackson2JsonMessageConverter()
}
