package no.fdk.harvestadmin.controller

import com.fasterxml.jackson.databind.ObjectMapper
import no.fdk.harvestadmin.model.DataSource
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.whenever
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.util.UUID

class DataSourceControllerTest : BaseControllerTest() {
    private val objectMapper = ObjectMapper()

    @Test
    fun `should get data sources`() {
        // Given
        val dataSource =
            DataSource(
                id = UUID.randomUUID().toString(),
                publisherId = "test-org",
                dataType = DataType.DATASET,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = "https://example.com/data",
            )
        whenever(securityService.getAuthorizedOrganizations(null)).thenReturn(listOf("test-org"))
        whenever(dataSourceService.getAllowedDataSources(listOf("test-org"), null, null)).thenReturn(listOf(dataSource))

        // When/Then
        mockMvc
            .perform(get("/datasources"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$[0].id").value(dataSource.id))
            .andExpect(jsonPath("$[0].publisherId").value("test-org"))
            .andExpect(jsonPath("$[0].dataType").value("dataset"))
    }

    @Test
    fun `should get data source by id`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataSource =
            DataSource(
                id = dataSourceId,
                publisherId = "test-org",
                dataType = DataType.DATASET,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = "https://example.com/data",
            )
        whenever(securityService.getAuthorizedOrganizations(null)).thenReturn(listOf("test-org"))
        whenever(dataSourceService.getDataSource(dataSourceId)).thenReturn(dataSource)

        // When/Then
        mockMvc
            .perform(get("/organizations/test-org/datasources/$dataSourceId"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id").value(dataSourceId))
            .andExpect(jsonPath("$.publisherId").value("test-org"))
    }

    @Test
    fun `should create data source`() {
        // Given
        val dataSource =
            DataSource(
                id = null,
                publisherId = "test-org",
                dataType = DataType.DATASET,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = "https://example.com/data",
            )
        val createdDataSource = dataSource.copy(id = UUID.randomUUID().toString())
        whenever(securityService.getAuthorizedOrganizations(null)).thenReturn(listOf("test-org"))
        whenever(dataSourceService.createDataSource(dataSource, "test-org")).thenReturn(createdDataSource)

        // When/Then
        mockMvc
            .perform(
                post("/organizations/test-org/datasources")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(dataSource)),
            ).andExpect(status().isCreated)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id").value(createdDataSource.id))
    }

    @Test
    fun `should update data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataSource =
            DataSource(
                id = dataSourceId,
                publisherId = "test-org",
                dataType = DataType.DATASET,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = "https://example.com/data-updated",
            )
        whenever(securityService.getAuthorizedOrganizations(any())).thenReturn(listOf("test-org"))
        whenever(dataSourceService.updateDataSource(any(), any(), any())).thenReturn(dataSource)

        // When/Then
        mockMvc
            .perform(
                put("/organizations/test-org/datasources/$dataSourceId")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(dataSource)),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.url").value("https://example.com/data-updated"))
    }

    @Test
    fun `should delete data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        whenever(securityService.getAuthorizedOrganizations(null)).thenReturn(listOf("test-org"))
        doNothing().whenever(dataSourceService).deleteDataSource(dataSourceId)

        // When/Then
        mockMvc
            .perform(delete("/organizations/test-org/datasources/$dataSourceId"))
            .andExpect(status().isNoContent)
    }
}
