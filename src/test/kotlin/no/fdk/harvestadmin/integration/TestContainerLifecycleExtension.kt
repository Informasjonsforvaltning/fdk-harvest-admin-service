package no.fdk.harvestadmin.integration

import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.opentest4j.TestAbortedException
import org.slf4j.LoggerFactory

/**
 * JUnit extension to ensure shared test containers are properly initialized
 * and remain available across all integration tests.
 */
class TestContainerLifecycleExtension : BeforeAllCallback {
    private val logger = LoggerFactory.getLogger(TestContainerLifecycleExtension::class.java)

    override fun beforeAll(context: ExtensionContext) {
        logger.info("🔧 Ensuring shared test containers are started...")

        try {
            // Ensure all containers are started
            if (!SharedTestContainers.postgresContainer.isRunning) {
                logger.info("🚀 Starting PostgreSQL container...")
                SharedTestContainers.postgresContainer.start()
            }

            if (!SharedTestContainers.kafkaContainer.isRunning) {
                logger.info("🚀 Starting Kafka container...")
                SharedTestContainers.kafkaContainer.start()
            }

            if (!SharedTestContainers.schemaRegistryContainer.isRunning) {
                logger.info("🚀 Starting Schema Registry container...")
                SharedTestContainers.schemaRegistryContainer.start()
            }

            logger.info("✅ All shared test containers are running")
            logger.info("   - PostgreSQL: ${SharedTestContainers.postgresContainer.jdbcUrl}")
            logger.info("   - Kafka: ${SharedTestContainers.kafkaContainer.bootstrapServers}")
            logger.info(
                "   - Schema Registry: " +
                    "http://${SharedTestContainers.schemaRegistryContainer.host}:" +
                    "${SharedTestContainers.schemaRegistryContainer.getMappedPort(8081)}",
            )
        } catch (e: Exception) {
            logger.error("❌ Failed to start test containers. Docker may not be available.", e)
            throw TestAbortedException(
                "Docker is not available. Skipping integration tests that require Docker.",
                e,
            )
        }
    }
}
