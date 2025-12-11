package no.fdk.harvestadmin.integration

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.kafka.KafkaHarvestEventConsumer
import no.fdk.harvestadmin.kafka.KafkaHarvestEventPublisher
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaIntegrationTest : BaseIntegrationTest() {
    @Autowired
    private lateinit var kafkaHarvestEventPublisher: KafkaHarvestEventPublisher

    @Autowired
    private lateinit var kafkaHarvestEventConsumer: KafkaHarvestEventConsumer

    @Autowired
    private lateinit var harvestEventRepository: HarvestEventRepository

    @Autowired
    private lateinit var harvestRunRepository: HarvestRunRepository

    @Test
    fun `should verify Kafka infrastructure is working`() {
        // Simple test to verify Kafka is working
        println("🔍 Testing Kafka infrastructure...")
        println("📋 Kafka bootstrap servers: ${kafkaContainer.bootstrapServers}")
        println("📋 Schema registry URL: http://${schemaRegistryContainer.host}:${schemaRegistryContainer.getMappedPort(8081)}")

        // Test basic Kafka connectivity
        val producerProps =
            Properties().apply {
                put("bootstrap.servers", kafkaContainer.bootstrapServers)
                put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            }

        val producer = KafkaProducer<String, String>(producerProps)
        try {
            val testRecord = ProducerRecord<String, String>("test-topic", "test-key", "test-value")
            producer.send(testRecord).get(5, TimeUnit.SECONDS)
            println("✅ Kafka producer test successful")
        } catch (e: Exception) {
            println("❌ Kafka producer test failed: ${e.message}")
            throw e
        } finally {
            producer.close()
        }
    }

    @Test
    @Transactional
    fun `should publish and consume harvest event via Kafka`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(no.fdk.harvest.DataType.dataset)
                .setStartTime(Instant.now().toString())
                .build()

        // When: Publish event
        kafkaHarvestEventPublisher.publishEvent(event)
        println("✅ Published harvest event to Kafka")

        // Wait for consumer to process
        Thread.sleep(3000)

        // Then: Verify event was persisted
        val persistedEvents = harvestEventRepository.findByDataSourceIdOrderByCreatedAtDesc(dataSourceId)
        assertNotNull(persistedEvents)
        assertEquals(1, persistedEvents.size)
        assertEquals(dataSourceId, persistedEvents[0].dataSourceId)
        assertEquals(runId, persistedEvents[0].runId)
        assertEquals("HARVESTING", persistedEvents[0].eventType)
        println("✅ Harvest event successfully persisted to database")
    }

    @Test
    @Transactional
    fun `should handle multiple harvest events for same data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()

        val events =
            listOf(
                HarvestEvent
                    .newBuilder()
                    .setPhase(HarvestPhase.INITIATING)
                    .setDataSourceId(dataSourceId)
                    .setRunId(runId)
                    .setDataType(no.fdk.harvest.DataType.dataset)
                    .build(),
                HarvestEvent
                    .newBuilder()
                    .setPhase(HarvestPhase.HARVESTING)
                    .setDataSourceId(dataSourceId)
                    .setRunId(runId)
                    .setDataType(no.fdk.harvest.DataType.dataset)
                    .setStartTime(Instant.now().toString())
                    .build(),
                HarvestEvent
                    .newBuilder()
                    .setPhase(HarvestPhase.REASONING)
                    .setDataSourceId(dataSourceId)
                    .setRunId(runId)
                    .setDataType(no.fdk.harvest.DataType.dataset)
                    .setStartTime(Instant.now().toString())
                    .setEndTime(Instant.now().plusSeconds(5).toString())
                    .build(),
            )

        // When: Publish all events
        events.forEach { event ->
            kafkaHarvestEventPublisher.publishEvent(event)
        }
        println("✅ Published ${events.size} harvest events to Kafka")

        // Wait for consumer to process
        Thread.sleep(5000)

        // Then: Verify all events were persisted
        val persistedEvents = harvestEventRepository.findByDataSourceIdOrderByCreatedAtDesc(dataSourceId)
        // Note: INITIATING events are skipped by the processor, so we expect 2 events
        assertEquals(2, persistedEvents.size)
        // Verify both events exist (order may vary due to async processing)
        val eventTypes = persistedEvents.map { it.eventType }.toSet()
        assert(eventTypes.contains("HARVESTING")) { "Expected HARVESTING event" }
        assert(eventTypes.contains("REASONING")) { "Expected REASONING event" }
        println("✅ All harvest events successfully persisted to database")
    }

    @Test
    @Transactional
    fun `should handle harvest event with resource counts`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.RESOURCE_PROCESSING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(no.fdk.harvest.DataType.concept)
                .setChangedResourcesCount(10)
                .setUnchangedResourcesCount(5)
                .setRemovedResourcesCount(2)
                .build()

        // When: Publish event
        kafkaHarvestEventPublisher.publishEvent(event)
        println("✅ Published harvest event with resource counts to Kafka")

        // Wait for consumer to process
        Thread.sleep(3000)

        // Then: Verify event was persisted with resource counts
        val persistedEvents = harvestEventRepository.findByDataSourceIdOrderByCreatedAtDesc(dataSourceId)
        assertEquals(1, persistedEvents.size)
        assertEquals(10, persistedEvents[0].changedResourcesCount)
        assertEquals(5, persistedEvents[0].unchangedResourcesCount)
        assertEquals(2, persistedEvents[0].removedResourcesCount)
        println("✅ Harvest event with resource counts successfully persisted")
    }

    @Test
    @Transactional
    fun `should handle harvest event with error message`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()
        val errorMessage = "Test error message"

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(no.fdk.harvest.DataType.dataset)
                .setErrorMessage(errorMessage)
                .build()

        // When: Publish event
        kafkaHarvestEventPublisher.publishEvent(event)
        println("✅ Published harvest event with error to Kafka")

        // Wait for consumer to process
        Thread.sleep(3000)

        // Then: Verify event was persisted with error message
        val persistedEvents = harvestEventRepository.findByDataSourceIdOrderByCreatedAtDesc(dataSourceId)
        assertEquals(1, persistedEvents.size)
        assertEquals(errorMessage, persistedEvents[0].errorMessage)
        println("✅ Harvest event with error message successfully persisted")
    }

    @Test
    fun `should produce and consume harvest event using direct Kafka producer`() {
        // Given
        val topic = "harvest-events"
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(no.fdk.harvest.DataType.dataset)
                .setStartTime(Instant.now().toString())
                .build()

        // Create Kafka producer
        val producerProps =
            Properties().apply {
                put("bootstrap.servers", kafkaContainer.bootstrapServers)
                put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
                put("schema.registry.url", "http://${schemaRegistryContainer.host}:${schemaRegistryContainer.getMappedPort(8081)}")
            }

        val producer = KafkaProducer<String, HarvestEvent>(producerProps)

        try {
            // When: Produce a message to Kafka
            val record = ProducerRecord<String, HarvestEvent>(topic, dataSourceId, event)
            producer.send(record).get(10, TimeUnit.SECONDS)
            println("✅ Produced message to topic: $topic")

            // Wait a bit for the consumer to process the message
            Thread.sleep(3000)

            // Then: Verify the event was stored in the database
            val persistedEvents = harvestEventRepository.findByDataSourceIdOrderByCreatedAtDesc(dataSourceId)
            assertNotNull(persistedEvents)
            assertEquals(1, persistedEvents.size)
            assertEquals(dataSourceId, persistedEvents[0].dataSourceId)
            assertEquals(runId, persistedEvents[0].runId)
            println("✅ Harvest event successfully stored and retrieved from database")
        } finally {
            producer.close()
        }
    }
}
