package no.fdk.harvestadmin.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.service.HarvestRunService
import org.springframework.http.ResponseEntity
import org.springframework.security.core.Authentication
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/internal/runs")
@Tag(name = "Harvest Run", description = "Harvest Run Service (Internal)")
class HarvestRunController(
    private val harvestRunService: HarvestRunService,
) {
    @GetMapping
    @Operation(
        summary = "List harvest runs (internal)",
        description =
            "Returns a paginated list of harvest runs sorted by runStartedAt (descending). " +
                "Can be filtered by dataSourceId, dataType, and status.",
        security = [SecurityRequirement(name = "api-key")],
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "Paginated list of harvest runs",
                content = [
                    Content(
                        mediaType = "application/json",
                        schema = Schema(implementation = Array<HarvestRunDetails>::class),
                    ),
                ],
            ),
        ],
    )
    fun listRuns(
        @Parameter(description = "Filter by data source ID") @RequestParam(required = false) dataSourceId: String?,
        @Parameter(description = "Filter by data type") @RequestParam(required = false) dataType: String?,
        @Parameter(description = "Filter by status (IN_PROGRESS, COMPLETED, FAILED)") @RequestParam(required = false) status: String?,
        @Parameter(description = "Number of records to skip (for pagination)") @RequestParam(required = false, defaultValue = "0") offset:
            Int,
        @Parameter(description = "Maximum number of runs to return") @RequestParam(required = false, defaultValue = "50") limit: Int,
        authentication: Authentication?,
    ): ResponseEntity<Map<String, Any>> {
        // Validate status if provided
        if (status != null && !listOf("IN_PROGRESS", "COMPLETED", "FAILED").contains(status)) {
            return ResponseEntity.badRequest().body(
                mapOf(
                    "error" to "Invalid status. Must be one of: IN_PROGRESS, COMPLETED, FAILED",
                ),
            )
        }

        // Validate pagination parameters
        if (offset < 0) {
            return ResponseEntity.badRequest().body(
                mapOf(
                    "error" to "offset must be >= 0",
                ),
            )
        }
        if (limit <= 0 || limit > 1000) {
            return ResponseEntity.badRequest().body(
                mapOf(
                    "error" to "limit must be between 1 and 1000",
                ),
            )
        }

        val (runs, totalCount) = harvestRunService.getHarvestRuns(dataSourceId, dataType, status, offset, limit)

        return ResponseEntity.ok(
            mapOf(
                "runs" to runs,
                "total" to totalCount,
                "offset" to offset,
                "limit" to limit,
            ),
        )
    }

    @GetMapping("/{runId}")
    @Operation(
        summary = "Get a specific harvest run (internal)",
        description = "Returns detailed information about a specific harvest run including phase durations",
        security = [SecurityRequirement(name = "api-key")],
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "Harvest run details",
                content = [Content(schema = Schema(implementation = HarvestRunDetails::class))],
            ),
            ApiResponse(responseCode = "404", description = "Run not found"),
        ],
    )
    fun getRun(
        @Parameter(description = "Harvest run ID (runId, UUID)") @PathVariable runId: String,
        authentication: Authentication?,
    ): ResponseEntity<Any> {
        val (run, httpStatus) = harvestRunService.getHarvestRun(runId)
        return if (run != null) {
            ResponseEntity.status(httpStatus).body(run)
        } else {
            ResponseEntity.status(httpStatus).build()
        }
    }

    @GetMapping("/metrics")
    @Operation(
        summary = "Get performance metrics (internal)",
        description =
            "Returns aggregated performance metrics. " +
                "Can filter by dataSourceId, dataType, date range, days back, or limit to last N runs.",
        security = [SecurityRequirement(name = "api-key")],
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "Performance metrics",
                content = [Content(schema = Schema(implementation = HarvestPerformanceMetrics::class))],
            ),
            ApiResponse(responseCode = "400", description = "Invalid date format"),
            ApiResponse(responseCode = "404", description = "No metrics found"),
        ],
    )
    fun getMetrics(
        @Parameter(description = "Filter by data source ID") @RequestParam(required = false) dataSourceId: String?,
        @Parameter(description = "Filter by data type (required if dataSourceId is provided)") @RequestParam(required = false) dataType:
            String?,
        @Parameter(
            description = "Number of days to look back (used if no date range or limit specified)",
        ) @RequestParam(required = false) daysBack: Int?,
        @Parameter(description = "Start date for date range (ISO-8601 format)") @RequestParam(required = false) startDate: String?,
        @Parameter(description = "End date for date range (ISO-8601 format)") @RequestParam(required = false) endDate: String?,
        @Parameter(description = "Limit to last N completed runs") @RequestParam(required = false) limit: Int?,
        authentication: Authentication?,
    ): ResponseEntity<Any> {
        val startDateInstant =
            startDate?.let {
                try {
                    java.time.Instant.parse(it)
                } catch (e: Exception) {
                    return ResponseEntity.badRequest().body(
                        mapOf(
                            "error" to "Invalid startDate format. Use ISO-8601 format (e.g., 2024-01-01T00:00:00Z)",
                        ),
                    )
                }
            }
        val endDateInstant =
            endDate?.let {
                try {
                    java.time.Instant.parse(it)
                } catch (e: Exception) {
                    return ResponseEntity.badRequest().body(
                        mapOf(
                            "error" to "Invalid endDate format. Use ISO-8601 format (e.g., 2024-01-01T00:00:00Z)",
                        ),
                    )
                }
            }

        val (metrics, httpStatus) =
            when {
                dataSourceId != null && dataType != null -> {
                    // Get metrics for a specific data source and data type
                    harvestRunService.getPerformanceMetrics(
                        dataSourceId,
                        dataType,
                        daysBack = daysBack,
                        startDate = startDateInstant,
                        endDate = endDateInstant,
                        limit = limit,
                    )
                }
                else -> {
                    // Get global metrics
                    harvestRunService.getAllPerformanceMetrics(
                        daysBack = daysBack,
                        startDate = startDateInstant,
                        endDate = endDateInstant,
                        limit = limit,
                    )
                }
            }

        return if (metrics != null) {
            ResponseEntity.status(httpStatus).body(metrics)
        } else {
            ResponseEntity.status(httpStatus).build()
        }
    }
}
