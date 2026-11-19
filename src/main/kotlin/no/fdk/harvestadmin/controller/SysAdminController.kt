package no.fdk.harvestadmin.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import no.fdk.harvestadmin.exception.ForbiddenException
import no.fdk.harvestadmin.exception.ValidationException
import no.fdk.harvestadmin.model.ForceHarvestRequest
import no.fdk.harvestadmin.model.ForceHarvestResponse
import no.fdk.harvestadmin.model.RemoveResourcesRequest
import no.fdk.harvestadmin.service.ForcedHarvestService
import no.fdk.harvestadmin.service.SecurityService
import no.fdk.harvestadmin.service.SysAdminService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.Authentication
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping
@Tag(name = "SysAdmin", description = "System admin operations")
class SysAdminController(
    private val sysAdminService: SysAdminService,
    private val securityService: SecurityService,
    private val forcedHarvestService: ForcedHarvestService,
) {
    @PostMapping("/resources/remove")
    @Operation(
        summary = "Remove resources (sys-admin)",
        description = "Publishes one Kafka HarvestEvent per resource with phase REMOVING.",
        security = [SecurityRequirement(name = "bearer-jwt"), SecurityRequirement(name = "api-key")],
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "202", description = "Accepted"),
            ApiResponse(responseCode = "400", description = "Bad Request"),
            ApiResponse(responseCode = "403", description = "Forbidden"),
        ],
    )
    fun removeResources(@RequestBody request: RemoveResourcesRequest, authentication: Authentication?): ResponseEntity<Void> {
        if (!securityService.hasSystemAdminAccess(authentication)) {
            throw ForbiddenException("Access denied")
        }

        if (request.resources.isEmpty()) {
            throw ValidationException("resources must not be empty")
        }

        sysAdminService.publishRemovingEvents(request.dataType, request.resources)
        return ResponseEntity.status(HttpStatus.ACCEPTED).build()
    }

    @PostMapping("/resources/force-harvest")
    @Operation(
        summary = "Force harvest of resources (sys-admin)",
        description = "Starts one harvest per data source of the given resources, updating them even if unchanged.",
        security = [SecurityRequirement(name = "bearer-jwt"), SecurityRequirement(name = "api-key")],
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "202", description = "Accepted"),
            ApiResponse(responseCode = "400", description = "Bad Request"),
            ApiResponse(responseCode = "403", description = "Forbidden"),
        ],
    )
    fun forceHarvest(@RequestBody request: ForceHarvestRequest, authentication: Authentication?): ResponseEntity<ForceHarvestResponse> {
        if (!securityService.hasSystemAdminAccess(authentication)) {
            throw ForbiddenException("Access denied")
        }

        if (request.fdkIds.isEmpty()) {
            throw ValidationException("fdkIds must not be empty")
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(forcedHarvestService.forceHarvest(request.dataType, request.fdkIds))
    }
}
