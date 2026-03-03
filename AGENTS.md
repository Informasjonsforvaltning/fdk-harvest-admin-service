# FDK Harvest Admin Service – Agent Guide

This document gives AI agents and contributors enough context to work effectively in this codebase.

## Project Overview

**fdk-harvest-admin-service** is a Spring Boot (Kotlin) application that replaces the original Go-based `fdk-harvest-admin`. It provides a REST API to register and list data sources for harvesting, and integrates with Kafka for harvest events (triggers and reports). It is part of the FDK (Felles datakatalog) harvest pipeline.

- **Architecture**: See [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) for system context.
- **Run locally**: `mvn spring-boot:run` (requires Docker for PostgreSQL and Kafka: `docker compose up -d`).
- **Tests**: `mvn verify` (unit tests with Surefire; integration tests with Failsafe and Testcontainers).

## Harvest process and Kafka

The service coordinates harvest runs via a single Kafka topic (`harvest-events`, configurable). All messages use the Avro type `HarvestEvent` (see `kafka/schemas/no.fdk.harvest.HarvestEvent.avsc`).

### Message flow

- **Publish (this service)**  
  When a harvest is started (API or scheduled job), the service creates a run, persists an **INITIATING** event, and **publishes** one `HarvestEvent` with `phase=INITIATING` to Kafka. That message is the trigger for the harvester (or other consumers) to begin work. Key fields: `runId`, `dataSourceId`, `dataType`, `dataSourceUrl`, `acceptHeader`, `removeAll`, `forced`.

- **Consume (this service)**  
  The service **consumes** all other harvest events from the same topic (phase ≠ INITIATING). For each event it: persists the event, updates the corresponding `HarvestRun` (counts, phase, status), and records metrics. INITIATING events on the topic are ignored by the consumer (the run was already created when we published the trigger).

### Phases

Phases are defined in the Avro enum `HarvestPhase` and in `HarvestPhaseConfig`. Order used for completion:

1. **INITIATING** – Trigger only; emitted by this service when starting a harvest. Not used for completion.
2. **HARVESTING** – Fetch from source; one or more events per run, often one “summary” event with `changedResourcesCount` / `removedResourcesCount`. No per-resource `fdkId`/`resourceUri` required.
3. **REASONING** – Reasoning over harvested data.
4. **RDF_PARSING** – Parse RDF.
5. **RESOURCE_PROCESSING** – Map and validate resources.
6. **SEARCH_PROCESSING** – Index for search.
7. **AI_SEARCH_PROCESSING** – AI-related search (optional, see below).
8. **SPARQL_PROCESSING** – SPARQL-related processing (optional, see below).

Phases 3–8 are “resource-processing” phases: events typically carry `fdkId` and/or `resourceUri`. Completion is derived from “at least N resources completed” per phase, where N comes from HARVESTING’s `changedResourcesCount + removedResourcesCount` (or totalResources). **Optional phases** (`AI_SEARCH_PROCESSING`, `SPARQL_PROCESSING`): if a run has no events for such a phase, it does not block completion; if it has events, that phase is required and counts must match.

### Run status and completion

- **IN_PROGRESS** – Run exists and not all required phases are complete (or a phase has failed).
- **COMPLETED** – All required phases have at least the expected number of successful events; optional phases with no events are ignored. Special case: if HARVESTING reports 0 changed and 0 removed, the run can complete after HARVESTING finishes successfully.
- **FAILED** – Any required phase has an event with `errorMessage` set.

Completion logic and per-phase details are in `HarvestRunService.checkIfAllPhasesComplete`; optional vs required phases in `HarvestPhaseConfig`. The API can expose `completionStatus` (e.g. on run details) so operators can see which phase or resource count is blocking completion.

### Event fields (summary)

- **All events**: `phase`, `runId`, `dataType`, `dataSourceId`, `startTime`, `endTime`, `errorMessage`.
- **INITIATING**: `removeAll`, `forced`, `dataSourceUrl`, `acceptHeader`; no resource counts.
- **HARVESTING**: `changedResourcesCount`, `removedResourcesCount` (optional but used for completion).
- **Resource-processing phases**: often `fdkId`, `resourceUri` to count completed resources per phase.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Kotlin 2.2.x, Java 21 |
| Framework | Spring Boot 3.5.x |
| Build | Maven |
| Database | PostgreSQL, JPA, Flyway |
| Messaging | Apache Kafka |
| Auth | Spring Security, OAuth2 (Keycloak), API key for internal endpoints |
| API docs | SpringDoc OpenAPI (Swagger UI at `/swagger-ui.html`) |
| Observability | Micrometer/Prometheus, Actuator |
| Testing | JUnit 5, Mockito (mockito-kotlin), Testcontainers (Postgres, Kafka) |

## Project Layout

```
src/main/kotlin/no/fdk/harvestadmin/
├── FdkHarvestAdminServiceApplication.kt   # Entry point, @EnableScheduling
├── config/                                # Security, Kafka, OpenAPI
├── controller/                            # REST controllers (datasources, harvest runs, internal)
├── converter/                             # JPA attribute converters (e.g. DataType, DataSourceType)
├── entity/                                # JPA entities (DataSource, HarvestRun, HarvestEvent)
├── exception/                             # GlobalExceptionHandler, custom exceptions
├── kafka/                                 # KafkaHarvestEventConsumer, KafkaHarvestEventPublisher
├── model/                                 # DTOs and domain models (DataSource, HarvestStatus, etc.)
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

- **Kafka**: Single topic for harvest events; this service **publishes** INITIATING (trigger) and **consumes** all other phases. See [Harvest process and Kafka](#harvest-process-and-kafka) above, `KafkaHarvestEventConsumer`, `KafkaHarvestEventPublisher`, and `KafkaConfig`.
- **Avro**: Schemas in `kafka/schemas/`; generated classes in `target/generated-sources/avro`.

### Errors and Security

- **GlobalExceptionHandler**: Central REST error handling; keep responses consistent (e.g. JSON with `error` or similar).
- **Security**: Don’t bypass `SecurityConfig`; all API endpoints require authentication (JWT or API key). When adding endpoints, use permitAll only for actuator/docs; otherwise use authenticated.

### Tests

- **Controller tests**: Extend `BaseControllerTest`, use `@WebMvcTest`, `MockMvc`, and `@MockBean` for services. Use `@ActiveProfiles("test")` and application-test config so security can be relaxed where needed.
- **Integration tests**: Use `@SpringBootTest`, Testcontainers (PostgreSQL, Kafka as needed), and shared setup (e.g. `BaseIntegrationTest`, `TestContainerLifecycleExtension`, `SharedTestContainers`).
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
- `mvn compile` – compile (fails on warnings via `-Werror`)
- `docker compose up -d` – start PostgreSQL and Kafka for local run

## Compilation

- **Warnings as errors**: Kotlin compiler uses `-Werror`; fix or suppress any compilation warnings before committing.
- **Check before changes**: Run `mvn compile` (or `mvn verify`) after making code changes to ensure no new warnings or errors.

## Files to Check When Changing Behavior

- **Security / auth**: `SecurityConfig.kt`, `ApiKeyAuthenticationFilter.kt`, `OpenApiConfig.kt`
- **API surface**: Controllers in `controller/`, `application.yml` (paths/context if any)
- **Harvest flow**: `HarvestRunService`, `HarvestEventProcessor`, `KafkaHarvestEventConsumer`/`Publisher`
- **Data model**: `entity/`, `model/`, `repository/`, Flyway migrations
