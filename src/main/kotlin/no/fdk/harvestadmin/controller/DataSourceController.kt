package no.fdk.harvestadmin.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import no.fdk.harvestadmin.exception.ValidationException
import no.fdk.harvestadmin.model.DataSource
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.service.DataSourceService
import no.fdk.harvestadmin.service.HarvestRunService
import no.fdk.harvestadmin.service.SecurityService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.Authentication
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import io.swagger.v3.oas.annotations.parameters.RequestBody as SwaggerRequestBody

@RestController
@RequestMapping
@Tag(name = "Data Source", description = "Data Source Service")
class DataSourceController(
    private val dataSourceService: DataSourceService,
    private val harvestRunService: HarvestRunService,
    private val securityService: SecurityService,
) {
    private val validPublisherIdPattern = Regex("^[a-zA-Z0-9-_]+$")

    private fun validateOrgId(org: String) {
        if (org.isBlank()) {
            throw ValidationException("Organization ID is required")
        }
        if (!validPublisherIdPattern.matches(org)) {
            throw ValidationException("Organization ID must contain only alphanumeric characters, hyphens, and underscores")
        }
    }

    @GetMapping("/datasources")
    @Operation(
        summary = "Query for data sources",
        description = "Returns a collection of matching data sources",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "OK"),
            ApiResponse(responseCode = "403", description = "Forbidden"),
        ],
    )
    fun getDataSources(
        @Parameter(description = "Filter by data type") @RequestParam(required = false) dataType: String?,
        @Parameter(description = "Filter by data source type") @RequestParam(required = false) dataSourceType: String?,
        authentication: Authentication?,
    ): ResponseEntity<List<DataSource>> {
        val authorizedOrgs = securityService.getAuthorizedOrganizations(authentication)
        val dataTypeEnum = dataType?.let { DataType.fromString(it) }
        val dataSourceTypeEnum = dataSourceType?.let { DataSourceType.fromString(it) }

        val sources = dataSourceService.getAllowedDataSources(authorizedOrgs, dataTypeEnum, dataSourceTypeEnum)
        return ResponseEntity.ok(sources)
    }

    @GetMapping("/internal/datasources")
    @Operation(
        summary = "Query for all data sources (internal)",
        description = "Returns all data sources without authorization checks",
        security = [SecurityRequirement(name = "api-key")],
    )
    fun getAllInternalDataSources(
        @RequestParam(required = false) dataType: String?,
        @RequestParam(required = false) dataSourceType: String?,
    ): ResponseEntity<List<DataSource>> {
        val dataTypeEnum = dataType?.let { DataType.fromString(it) }
        val dataSourceTypeEnum = dataSourceType?.let { DataSourceType.fromString(it) }

        val sources = dataSourceService.getAllowedDataSources(null, dataTypeEnum, dataSourceTypeEnum)
        return ResponseEntity.ok(sources)
    }

    @GetMapping("/organizations/{org}/datasources")
    @Operation(
        summary = "Query for data sources by organization",
        description = "Returns a collection of data sources for specified organization",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    fun getOrgDataSources(
        @PathVariable org: String,
        @RequestParam(required = false) dataType: String?,
        @RequestParam(required = false) dataSourceType: String?,
        authentication: Authentication?,
    ): ResponseEntity<List<DataSource>> {
        validateOrgId(org)
        val authorizedOrgs = listOf(org)
        val dataTypeEnum = dataType?.let { DataType.fromString(it) }
        val dataSourceTypeEnum = dataSourceType?.let { DataSourceType.fromString(it) }

        val sources = dataSourceService.getAllowedDataSources(authorizedOrgs, dataTypeEnum, dataSourceTypeEnum)
        return ResponseEntity.ok(sources)
    }

    @GetMapping("/internal/organizations/{org}/datasources")
    @Operation(
        summary = "Query for data sources by organization (internal)",
        description = "Returns data sources for organization without authorization checks",
        security = [SecurityRequirement(name = "api-key")],
    )
    fun getInternalOrgDataSources(
        @PathVariable org: String,
        @RequestParam(required = false) dataType: String?,
        @RequestParam(required = false) dataSourceType: String?,
    ): ResponseEntity<List<DataSource>> {
        validateOrgId(org)
        val authorizedOrgs = listOf(org)
        val dataTypeEnum = dataType?.let { DataType.fromString(it) }
        val dataSourceTypeEnum = dataSourceType?.let { DataSourceType.fromString(it) }

        val sources = dataSourceService.getAllowedDataSources(authorizedOrgs, dataTypeEnum, dataSourceTypeEnum)
        return ResponseEntity.ok(sources)
    }

    @GetMapping("/organizations/{org}/datasources/{id}")
    @Operation(
        summary = "Get a specific data source by id",
        description = "Returns a data source by id",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    fun getDataSource(
        @PathVariable org: String,
        @PathVariable id: String,
        authentication: Authentication?,
    ): ResponseEntity<DataSource> {
        validateOrgId(org)
        val source = dataSourceService.getDataSource(id)
        return ResponseEntity.ok(source)
    }

    @GetMapping("/internal/organizations/{org}/datasources/{id}")
    @Operation(
        summary = "Get a specific data source by id (internal)",
        description = "Returns a data source by id without authorization checks",
        security = [SecurityRequirement(name = "api-key")],
    )
    fun getInternalDataSource(
        @PathVariable org: String,
        @PathVariable id: String,
    ): ResponseEntity<DataSource> {
        validateOrgId(org)
        val source = dataSourceService.getDataSource(id)
        return ResponseEntity.ok(source)
    }

    @PostMapping("/organizations/{org}/datasources")
    @Operation(
        summary = "Create a new data source",
        description = "Creates a new data source for the specified organization",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "201", description = "Created"),
            ApiResponse(responseCode = "400", description = "Bad Request"),
            ApiResponse(responseCode = "403", description = "Forbidden"),
            ApiResponse(responseCode = "409", description = "Conflict"),
        ],
    )
    fun createDataSource(
        @PathVariable org: String,
        @SwaggerRequestBody(
            description = "Data source to create",
            required = true,
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = DataSource::class),
                    examples = [
                        ExampleObject(
                            name = "Dataset data source",
                            value = """
                            {
                              "dataSourceType": "DCAT-AP-NO",
                              "dataType": "dataset",
                              "url": "https://example.com/datasets",
                              "acceptHeaderValue": "application/rdf+xml",
                              "publisherId": "example-org",
                              "description": "Example dataset catalog"
                            }
                            """,
                        ),
                        ExampleObject(
                            name = "Concept data source",
                            value = """
                            {
                              "dataSourceType": "SKOS",
                              "dataType": "concept",
                              "url": "https://example.com/concepts",
                              "publisherId": "example-org",
                              "description": "Example concept scheme"
                            }
                            """,
                        ),
                    ],
                ),
            ],
        )
        @Valid
        @RequestBody dataSource: DataSource,
        authentication: Authentication?,
    ): ResponseEntity<DataSource> {
        validateOrgId(org)
        val created = dataSourceService.createDataSource(dataSource, org)
        val location = "/organizations/$org/datasources/${created.id}"
        return ResponseEntity
            .status(HttpStatus.CREATED)
            .header("Location", location)
            .body(created)
    }

    @PutMapping("/organizations/{org}/datasources/{id}")
    @Operation(
        summary = "Update a specific data source by id",
        description = "Updates a data source by id",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    fun updateDataSource(
        @PathVariable org: String,
        @PathVariable id: String,
        @SwaggerRequestBody(
            description = "Updated data source (id field is ignored)",
            required = true,
            content = [
                Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = DataSource::class),
                    examples = [
                        ExampleObject(
                            name = "Update data source",
                            value = """
                            {
                              "dataSourceType": "DCAT-AP-NO",
                              "dataType": "dataset",
                              "url": "https://example.com/datasets-updated",
                              "acceptHeaderValue": "application/rdf+xml",
                              "publisherId": "example-org",
                              "description": "Updated dataset catalog"
                            }
                            """,
                        ),
                    ],
                ),
            ],
        )
        @Valid
        @RequestBody dataSource: DataSource,
        authentication: Authentication?,
    ): ResponseEntity<DataSource> {
        validateOrgId(org)
        val updated = dataSourceService.updateDataSource(id, dataSource, org)
        return ResponseEntity.ok(updated)
    }

    @DeleteMapping("/organizations/{org}/datasources/{id}")
    @Operation(
        summary = "Delete a specific data source by id",
        description = "Deletes a data source by id",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    fun deleteDataSource(
        @PathVariable org: String,
        @PathVariable id: String,
        authentication: Authentication?,
    ): ResponseEntity<Void> {
        validateOrgId(org)
        dataSourceService.deleteDataSource(id)
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }

    @PostMapping("/organizations/{org}/datasources/{id}/start-harvesting")
    @Operation(
        summary = "Start harvesting for a data source",
        description = "Triggers harvesting for a specific data source",
        security = [SecurityRequirement(name = "bearer-jwt")],
    )
    fun startHarvesting(
        @PathVariable org: String,
        @PathVariable id: String,
        authentication: Authentication?,
    ): ResponseEntity<Void> {
        validateOrgId(org)
        dataSourceService.startHarvesting(id, org)
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }

    @GetMapping("/internal/organizations/{org}/datasources/{id}/status")
    @Operation(
        summary = "Get harvest status for a data source (internal)",
        description =
            "Returns the current harvest state(s) for a data source. " +
                "Returns a list with one state per data type if dataType is not specified.",
        security = [SecurityRequirement(name = "api-key")],
    )
    fun getHarvestStatus(
        @PathVariable org: String,
        @PathVariable id: String,
        @Parameter(description = "Filter by data type") @RequestParam(required = false) dataType: String?,
        authentication: Authentication?,
    ): ResponseEntity<List<no.fdk.harvestadmin.model.HarvestCurrentState>> {
        validateOrgId(org)
        val (states, httpStatus) = harvestRunService.getCurrentState(id, dataType)
        return ResponseEntity.status(httpStatus).body(states)
    }
}
