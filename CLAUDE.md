# FDK Harvest Admin Service

Spring Boot 3 (Kotlin) service that manages data sources and harvest runs for the FDK harvest pipeline. Replaces the Go-based fdk-harvest-admin.

## Quick reference

- **Run**: `mvn spring-boot:run` (start Postgres + RabbitMQ first: `docker compose up -d`)
- **Test**: `mvn verify` (unit + integration with Testcontainers)
- **API docs**: http://localhost:8080/swagger-ui.html

## Stack

Kotlin 2.2, Java 21, Spring Boot 3.5, PostgreSQL (JPA + Flyway), RabbitMQ, Kafka (Avro). Auth: OAuth2 (Keycloak) + API key for `/internal/*`.

## Layout

- `src/main/kotlin/no/fdk/harvestadmin/`: `config/`, `controller/`, `entity/`, `model/`, `repository/`, `service/`, `kafka/`, `rabbit/`, `exception/`, `converter/`
- `src/main/resources/db/migration/`: Flyway SQL
- `kafka/schemas/`: Avro schemas (generated code in `target/generated-sources/avro`)
- Controller tests extend `BaseControllerTest`; integration tests use Testcontainers and `*IntegrationTest.kt`

## Conventions

- Constructor injection; document REST with SpringDoc (`@Operation`, `@Tag`, `@ApiResponses`, `@SecurityRequirement`).
- Internal endpoints under `/internal/` require API key; see `SecurityConfig` and `ApiKeyAuthenticationFilter`.
- DB changes: new Flyway migration only. Kafka changes: edit Avro in `kafka/schemas/`, then update consumer/publisher.
- ktlint runs in build; keep Kotlin style consistent.

## Key files

- **Security**: `SecurityConfig.kt`, `ApiKeyAuthenticationFilter.kt`
- **Harvest**: `HarvestRunService.kt`, `HarvestEventProcessor.kt`, Kafka consumer/publisher, Rabbit listener/publisher
- **API**: `HarvestRunController.kt`, `DataSourceController.kt`; errors: `GlobalExceptionHandler.kt`
