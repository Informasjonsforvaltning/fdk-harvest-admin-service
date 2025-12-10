package no.fdk.harvestadmin.integration

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Shared test containers that are reused across all integration tests.
 *
 * This singleton ensures that all integration tests use the same container instances,
 * preventing the issue where one test terminates containers needed by another test.
 */
object SharedTestContainers {
    private val network = Network.newNetwork()

    val postgresContainer: PostgreSQLContainer<*> by lazy {
        try {
            PostgreSQLContainer("postgres:15")
                .withDatabaseName("fdk_harvest_admin")
                .withUsername("postgres")
                .withPassword("postgres")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withReuse(true)
                .apply { start() }
        } catch (e: Exception) {
            throw IllegalStateException("Failed to start PostgreSQL container. Docker may not be available.", e)
        }
    }

    val kafkaContainer: KafkaContainer by lazy {
        try {
            KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withReuse(true)
                .apply { start() }
        } catch (e: Exception) {
            throw IllegalStateException("Failed to start Kafka container. Docker may not be available.", e)
        }
    }

    val schemaRegistryContainer: GenericContainer<*> by lazy {
        try {
            GenericContainer("confluentinc/cp-schema-registry:7.4.0")
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withExposedPorts(8081)
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                .dependsOn(kafkaContainer)
                .withReuse(true)
                .apply { start() }
        } catch (e: Exception) {
            throw IllegalStateException("Failed to start Schema Registry container. Docker may not be available.", e)
        }
    }

    /**
     * Get all containers as a list for cleanup
     */
    fun getAllContainers(): List<org.testcontainers.containers.Container<*>> =
        listOf(postgresContainer, kafkaContainer, schemaRegistryContainer)

    /**
     * Stop all containers (for cleanup)
     */
    fun stopAll() {
        try {
            schemaRegistryContainer.stop()
        } catch (e: Exception) {
            // Ignore stop errors
        }
        try {
            kafkaContainer.stop()
        } catch (e: Exception) {
            // Ignore stop errors
        }
        try {
            postgresContainer.stop()
        } catch (e: Exception) {
            // Ignore stop errors
        }
    }
}

