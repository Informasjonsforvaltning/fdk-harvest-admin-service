package no.fdk.harvestadmin.integration

import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.junit.jupiter.Testcontainers

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@ContextConfiguration(initializers = [BaseIntegrationTest.Companion.Initializer::class])
@ExtendWith(TestContainerLifecycleExtension::class)
abstract class BaseIntegrationTest {
    companion object {
        // Use shared containers to prevent one test from terminating containers needed by another
        val postgresContainer by lazy { SharedTestContainers.postgresContainer }
        val kafkaContainer by lazy { SharedTestContainers.kafkaContainer }
        val schemaRegistryContainer by lazy { SharedTestContainers.schemaRegistryContainer }

        class Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
            override fun initialize(context: ConfigurableApplicationContext) {
                TestPropertyValues
                    .of(
                        "spring.datasource.url=${postgresContainer.jdbcUrl}",
                        "spring.datasource.username=${postgresContainer.username}",
                        "spring.datasource.password=${postgresContainer.password}",
                        "spring.datasource.driver-class-name=org.postgresql.Driver",
                        "spring.kafka.bootstrap-servers=${kafkaContainer.bootstrapServers}",
                        "spring.kafka.consumer.group-id=test-group",
                        "spring.kafka.consumer.auto-offset-reset=earliest",
                        "spring.kafka.consumer.enable-auto-commit=false",
                        "spring.kafka.listener.ack-mode=manual",
                        "spring.kafka.properties.schema.registry.url=" +
                            "http://${schemaRegistryContainer.host}:${schemaRegistryContainer.getMappedPort(8081)}",
                        "app.kafka.topic.harvest-events=harvest-events",
                        "spring.autoconfigure.exclude=org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration,org.springframework.boot.security.oauth2.server.resource.autoconfigure.OAuth2ResourceServerAutoConfiguration",
                        "spring.kafka.enabled=true",
                        "spring.kafka.listener.auto-startup=true",
                        "logging.level.org.apache.kafka=DEBUG",
                        "logging.level.org.springframework.kafka=DEBUG",
                        "logging.level.no.fdk.harvestadmin.kafka=DEBUG",
                        "logging.level.org.springframework.kafka.listener=DEBUG",
                        "logging.level.org.springframework.kafka.listener.ContainerProperties=DEBUG",
                    ).applyTo(context.environment)
            }
        }
    }
}
