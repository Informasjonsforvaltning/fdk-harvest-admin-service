# FDK Harvest Admin Service

This application provides an API to register and list data sources to be harvested. It is a Spring Boot replacement for the original Go-based `fdk-harvest-admin` application.

For a broader understanding of the system's context, refer to the [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) wiki.

## Features

- **REST API** for managing data sources (create, read, update, delete)
- **RabbitMQ integration** for consuming harvest reports and publishing harvest triggers
- **PostgreSQL database** for persistent storage
- **OAuth2 authentication** with Keycloak integration
- **API key authentication** for internal endpoints
- **Harvest status tracking** with support for harvested, reasoned, and ingested reports

## Getting Started

### Prerequisites

Ensure you have the following installed:

- Java 21
- Maven
- Docker

### Running locally

1. Clone the repository and navigate to the project directory:

```sh
cd fdk-harvest-admin-service
```

2. Start PostgreSQL and RabbitMQ:

```sh
docker compose up -d
```

3. Run the application:

```sh
mvn spring-boot:run
```

The application will start on `http://localhost:8080`.

### API Documentation (OpenAPI)

Once the application is running locally, the API documentation can be accessed at:
- Swagger UI: http://localhost:8080/swagger-ui.html
- OpenAPI JSON: http://localhost:8080/api-docs

### Running tests

```sh
mvn verify
```

## Configuration

The application can be configured using environment variables or `application.yml`. Key configuration options:

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD` - Database configuration
- `RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USERNAME`, `RABBIT_PASSWORD` - RabbitMQ configuration
- `RABBIT_EXCHANGE`, `RABBIT_QUEUE` - RabbitMQ exchange and queue names
- `SSO_AUTH_URI` - Keycloak authentication server URI
- `TOKEN_AUDIENCE` - OAuth2 token audience (default: `fdk-harvest-admin`)
- `API_KEY` - API key for internal endpoints
- `CORS_ORIGIN_PATTERNS` - CORS allowed origin patterns (comma-separated)

## API Endpoints

### Public Endpoints
- `GET /ping` - Health check
- `GET /ready` - Readiness probe

### Authenticated Endpoints
- `GET /datasources` - Get all data sources (filtered by authorization)
- `GET /organizations/{org}/datasources` - Get data sources for an organization
- `GET /organizations/{org}/datasources/{id}` - Get a specific data source
- `POST /organizations/{org}/datasources` - Create a new data source
- `PUT /organizations/{org}/datasources/{id}` - Update a data source
- `DELETE /organizations/{org}/datasources/{id}` - Delete a data source
- `GET /organizations/{org}/datasources/{id}/status` - Get harvest status
- `POST /organizations/{org}/datasources/{id}/start-harvesting` - Trigger harvest

### Internal Endpoints (API Key Required)
- `GET /internal/datasources` - Get all data sources
- `GET /internal/organizations/{org}/datasources` - Get data sources for an organization
- `GET /internal/organizations/{org}/datasources/{id}` - Get a specific data source

## RabbitMQ Integration

The service consumes messages from RabbitMQ with the following routing keys:
- `*.publisher.NewDataSource` - New data source creation events
- `*.harvested` - Harvest completion reports
- `*.reasoned` - Reasoning completion reports
- `*.ingested` - Ingestion completion reports

The service publishes harvest trigger messages with routing keys:
- `{datatype}.publisher.HarvestTrigger` - Triggers harvesting for a data source

## Database Schema

### data_sources
Stores data source configurations with the following fields:
- `id` - Unique identifier (UUID)
- `data_source_type` - Type of data source (SKOS-AP-NO, DCAT-AP-NO, etc.)
- `data_type` - Type of data (concept, dataset, etc.)
- `url` - URL of the data source
- `accept_header_value` - Accept header for HTTP requests
- `publisher_id` - Organization ID
- `description` - Optional description
- `auth_header` - Optional authentication header (JSON)

### harvest_reports
Stores harvest status reports:
- `id` - Report identifier (data source ID or special IDs like "reasoning-{id}" or "ingested")
- `reports` - JSON object containing harvest reports by data type

## Migration from Go Application

This Spring Boot application replaces the original Go-based `fdk-harvest-admin`. It maintains API compatibility and supports the same endpoints and RabbitMQ integration patterns.


