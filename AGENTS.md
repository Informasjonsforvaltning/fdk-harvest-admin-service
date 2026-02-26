# FDK Harvest Admin Service – Agent Guide

This document gives AI agents and contributors enough context to work effectively in this codebase.

## Project Overview

**fdk-harvest-admin-service** is a Spring Boot (Kotlin) application that replaces the original Go-based `fdk-harvest-admin`. It provides a REST API to register and list data sources for harvesting, and integrates with RabbitMQ (harvest triggers/reports) and Kafka (harvest events). It is part of the FDK (Felles datakatalog) harvest pipeline.

- **Architecture**: See [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) for system context.
- **Run locally**: `mvn spring-boot:run` (requires Docker for PostgreSQL and RabbitMQ: `docker compose up -d`).
- **Tests**: `mvn verify` (unit tests with Surefire; integration tests with Failsafe and Testcontainers).

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Kotlin 2.2.x, Java 21 |
| Framework | Spring Boot 3.5.x |
| Build | Maven |
| Database | PostgreSQL, JPA, Flyway |
| Messaging | RabbitMQ (AMQP), Apache Kafka |
| Auth | Spring Security, OAuth2 (Keycloak), API key for internal endpoints |
| API docs | SpringDoc OpenAPI (Swagger UI at `/swagger-ui.html`) |
| Observability | Micrometer/Prometheus, Actuator |
| Testing | JUnit 5, Mockito (mockito-kotlin), Testcontainers (Postgres, RabbitMQ, Kafka) |

## Project Layout

```
src/main/kotlin/no/fdk/harvestadmin/
├── FdkHarvestAdminServiceApplication.kt   # Entry point, @EnableScheduling
├── config/                                # Security, Kafka, RabbitMQ, OpenAPI
├── controller/                            # REST controllers (datasources, harvest runs, internal)
├── converter/                             # JPA attribute converters (e.g. DataType, DataSourceType)
├── entity/                                # JPA entities (DataSource, HarvestRun, HarvestEvent)
├── exception/                             # GlobalExceptionHandler, custom exceptions
├── kafka/                                 # KafkaHarvestEventConsumer, KafkaHarvestEventPublisher
├── model/                                 # DTOs and domain models (DataSource, HarvestStatus, etc.)
├── rabbit/                                # RabbitMQListener, RabbitMQPublisher
├── repository/                            # Spring Data JPA repositories
└── service/                               # Business logic (DataSourceService, HarvestRunService, etc.)

src/main/resources/
├── application.yml
└── db/migration/                          # Flyway migrations (V1__, V2__, ...)

src/test/kotlin/no/fdk/harvestadmin/
├── controller/                            # WebMvcTest controller tests (BaseControllerTest)
├── integration/                           # Full-context integration tests, Testcontainers
├── kafka/                                 # Kafka consumer/publisher tests
└── service/                               # Service unit tests

kafka/schemas/                             # Avro schemas (e.g. HarvestEvent.avsc); compiled to target/generated-sources/avro
deploy/                                    # Kustomize (base, staging, prod, demo)
scripts/                                   # Migration and utility scripts (e.g. migrate-mongo-to-postgres)
```

## Conventions and Patterns

### Kotlin and Spring

- **Package**: `no.fdk.harvestadmin`; subpackages by layer (`controller`, `service`, `repository`, etc.).
- Prefer **constructor injection**; use `@RestController`, `@Service`, `@Repository` as appropriate.
- Use **data classes** for DTOs and domain models where it fits.
- **Linting**: ktlint runs in the build (format + check at test-compile).

### REST API

- **Paths**: Data source endpoints under `/organizations/{org}/datasources` and `/datasources`; harvest runs under `/runs`. All require authentication (JWT or API key).
- **Auth**: OAuth2 (Keycloak) for JWT; API key (e.g. `X-API-Key`) for service calls; both are accepted on all authenticated endpoints. See `SecurityConfig` and `ApiKeyAuthenticationFilter`.
- **OpenAPI**: Use `@Tag`, `@Operation`, `@ApiResponses`, `@Parameter`, `@SecurityRequirement` on controllers so Swagger stays accurate.

### Persistence

- **Flyway**: All schema changes go in `src/main/resources/db/migration/` with `V<n>__Description.sql`.
- **Converters**: Use `@Converter` (e.g. in `converter/`) for enum-like or custom types stored in the DB.

### Messaging

- **RabbitMQ**: Consumes harvest reports (e.g. `*.harvested`, `*.reasoned`, `*.ingested`) and publishes harvest triggers (`{datatype}.publisher.HarvestTrigger`). See `RabbitMQListener`, `RabbitMQPublisher`, and README.
- **Kafka**: Consumes/publishes harvest events; Avro schemas in `kafka/schemas/`; generated classes in `target/generated-sources/avro`. See `KafkaHarvestEventConsumer`, `KafkaHarvestEventPublisher`, and `KafkaConfig`.

### Errors and Security

- **GlobalExceptionHandler**: Central REST error handling; keep responses consistent (e.g. JSON with `error` or similar).
- **Security**: Don’t bypass `SecurityConfig`; all API endpoints require authentication (JWT or API key). When adding endpoints, use permitAll only for actuator/docs; otherwise use authenticated.

### Tests

- **Controller tests**: Extend `BaseControllerTest`, use `@WebMvcTest`, `MockMvc`, and `@MockBean` for services. Use `@ActiveProfiles("test")` and application-test config so security can be relaxed where needed.
- **Integration tests**: Use `@SpringBootTest`, Testcontainers (PostgreSQL, RabbitMQ, Kafka as needed), and shared setup (e.g. `BaseIntegrationTest`, `TestContainerLifecycleExtension`, `SharedTestContainers`).
- **Naming**: `*Test.kt` for unit tests (Surefire); `*IntegrationTest.kt` for integration tests (Failsafe).

## Common Tasks

- **Add a new REST endpoint**: Add method in the right controller; document with OpenAPI annotations; add or reuse security rule in `SecurityConfig`; add controller (and optionally integration) tests.
- **Change DB schema**: Add a Flyway migration under `db/migration/`; update entity and any converters if needed.
- **Change Kafka event shape**: Update Avro schema in `kafka/schemas/`; rebuild and update consumer/publisher and any tests.
- **Add a new internal (API-key) endpoint**: Put it under a path permitted for API key in `SecurityConfig` (e.g. under `/internal/`) and annotate with `@SecurityRequirement(name = "api-key")` in OpenAPI.

## Useful Commands

- `mvn spring-boot:run` – run the application
- `mvn verify` – run all tests (unit + integration)
- `mvn test` – run unit tests only (excludes `*IntegrationTest`)
- `docker compose up -d` – start PostgreSQL and RabbitMQ for local run

## Files to Check When Changing Behavior

- **Security / auth**: `SecurityConfig.kt`, `ApiKeyAuthenticationFilter.kt`, `OpenApiConfig.kt`
- **API surface**: Controllers in `controller/`, `application.yml` (paths/context if any)
- **Harvest flow**: `HarvestRunService`, `HarvestEventProcessor`, `KafkaHarvestEventConsumer`/`Publisher`, `RabbitMQListener`/`Publisher`
- **Data model**: `entity/`, `model/`, `repository/`, Flyway migrations
