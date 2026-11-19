package no.fdk.harvestadmin.controller

import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ForceHarvestRequest
import no.fdk.harvestadmin.model.ForceHarvestResponse
import no.fdk.harvestadmin.model.ForcedHarvestRun
import no.fdk.harvestadmin.model.RemoveResourcesRequest
import no.fdk.harvestadmin.model.ResourceToRemove
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.eq
import org.mockito.kotlin.isNull
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import tools.jackson.databind.json.JsonMapper

class SysAdminControllerTest : BaseControllerTest() {
    private val objectMapper = JsonMapper.builder().build()

    @Test
    fun `should return 202 and publish events for valid request`() {
        val request =
            RemoveResourcesRequest(
                dataType = DataType.DATASET,
                resources =
                listOf(
                    ResourceToRemove(fdkId = "fdk-1", resourceUri = "https://example.com/r/1"),
                    ResourceToRemove(fdkId = "fdk-2", resourceUri = "https://example.com/r/2"),
                ),
            )

        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(true)
        doNothing().whenever(sysAdminService).publishRemovingEvents(eq(DataType.DATASET), eq(request.resources))

        mockMvc
            .perform(
                post("/resources/remove")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isAccepted)

        verify(sysAdminService).publishRemovingEvents(eq(DataType.DATASET), eq(request.resources))
    }

    @Test
    fun `should return 403 when access check denies the request`() {
        val request =
            RemoveResourcesRequest(
                dataType = DataType.DATASET,
                resources =
                listOf(
                    ResourceToRemove(fdkId = "fdk-1", resourceUri = "https://example.com/r/1"),
                ),
            )

        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(false)

        mockMvc
            .perform(
                post("/resources/remove")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isForbidden)
            .andExpect(jsonPath("$.error").value("Access denied"))
    }

    @Test
    fun `should return 400 when resources list is empty`() {
        val request =
            RemoveResourcesRequest(
                dataType = DataType.CONCEPT,
                resources = emptyList(),
            )

        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(true)

        mockMvc
            .perform(
                post("/resources/remove")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.error").value("resources must not be empty"))
    }

    @Test
    fun `should return 202 with started runs for force harvest`() {
        val request = ForceHarvestRequest(dataType = DataType.DATASET, fdkIds = listOf("fdk-1", "fdk-2"))
        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(true)
        whenever(forcedHarvestService.forceHarvest(eq(DataType.DATASET), eq(request.fdkIds))).thenReturn(
            ForceHarvestResponse(startedRuns = listOf(ForcedHarvestRun("ds-1", listOf("fdk-1"))), notFound = listOf("fdk-2")),
        )

        mockMvc
            .perform(
                post("/resources/force-harvest")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isAccepted)
            .andExpect(jsonPath("$.startedRuns[0].dataSourceId").value("ds-1"))
            .andExpect(jsonPath("$.notFound[0]").value("fdk-2"))
    }

    @Test
    fun `should return 403 for force harvest without sys admin access`() {
        val request = ForceHarvestRequest(dataType = DataType.DATASET, fdkIds = listOf("fdk-1"))
        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(false)

        mockMvc
            .perform(
                post("/resources/force-harvest")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isForbidden)
    }

    @Test
    fun `should return 400 for force harvest without fdk ids`() {
        val request = ForceHarvestRequest(dataType = DataType.DATASET, fdkIds = emptyList())
        whenever(securityService.hasSystemAdminAccess(isNull())).thenReturn(true)

        mockMvc
            .perform(
                post("/resources/force-harvest")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(request)),
            ).andExpect(status().isBadRequest)
    }
}
