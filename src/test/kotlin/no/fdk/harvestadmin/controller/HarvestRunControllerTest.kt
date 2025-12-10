package no.fdk.harvestadmin.controller

import no.fdk.harvestadmin.model.HarvestPerformanceMetrics
import no.fdk.harvestadmin.model.HarvestRunDetails
import no.fdk.harvestadmin.model.PhaseDurations
import no.fdk.harvestadmin.model.ResourceCounts
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.time.Instant
import java.util.UUID

class HarvestRunControllerTest : BaseControllerTest() {
    @Test
    fun `should list all in-progress runs`() {
        // Given
        val runDetails = createHarvestRunDetails()
        whenever(harvestRunService.getAllInProgressStates()).thenReturn(listOf(runDetails))

        // When/Then
        mockMvc
            .perform(get("/internal/runs").param("status", "IN_PROGRESS"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$[0].id").value(runDetails.id))
            .andExpect(jsonPath("$[0].dataSourceId").value(runDetails.dataSourceId))
            .andExpect(jsonPath("$[0].status").value("IN_PROGRESS"))
    }

    @Test
    fun `should list runs by data source id`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runDetails = createHarvestRunDetails(dataSourceId = dataSourceId)
        whenever(harvestRunService.getHarvestRuns(eq(dataSourceId), anyOrNull(), any())).thenReturn(listOf(runDetails))

        // When/Then
        mockMvc
            .perform(get("/internal/runs").param("dataSourceId", dataSourceId))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$[0].dataSourceId").value(dataSourceId))
    }

    @Test
    fun `should list runs by data source id and data type`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataType = "dataset"
        val runDetails = createHarvestRunDetails(dataSourceId = dataSourceId, dataType = dataType)
        whenever(harvestRunService.getHarvestRuns(eq(dataSourceId), anyOrNull(), any())).thenReturn(listOf(runDetails))

        // When/Then
        mockMvc
            .perform(
                get("/internal/runs")
                    .param("dataSourceId", dataSourceId)
                    .param("dataType", dataType),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$[0].dataSourceId").value(dataSourceId))
            .andExpect(jsonPath("$[0].dataType").value(dataType))
    }

    @Test
    fun `should return empty list when no filters provided`() {
        // When/Then
        mockMvc
            .perform(get("/internal/runs"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$").isEmpty)
    }

    @Test
    fun `should get harvest run by id`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val runDetails = createHarvestRunDetails()
        whenever(harvestRunService.getHarvestRun(eq(runId))).thenReturn(Pair(runDetails, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(get("/internal/runs/$runId"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id").value(runDetails.id))
            .andExpect(jsonPath("$.dataSourceId").value(runDetails.dataSourceId))
    }

    @Test
    fun `should return 404 when harvest run not found`() {
        // Given
        val runId = UUID.randomUUID().toString()
        whenever(harvestRunService.getHarvestRun(eq(runId))).thenReturn(Pair(null, HttpStatus.NOT_FOUND))

        // When/Then
        mockMvc
            .perform(get("/internal/runs/$runId"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `should get metrics for specific data source and data type`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataType = "dataset"
        val metrics = createHarvestPerformanceMetrics(dataSourceId = dataSourceId, dataType = dataType)
        whenever(
            harvestRunService.getPerformanceMetrics(
                eq(dataSourceId),
                eq(dataType),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(metrics, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(
                get("/internal/runs/metrics")
                    .param("dataSourceId", dataSourceId)
                    .param("dataType", dataType),
            ).andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.dataSourceId").value(dataSourceId))
            .andExpect(jsonPath("$.dataType").value(dataType))
            .andExpect(jsonPath("$.totalRuns").value(metrics.totalRuns))
    }

    @Test
    fun `should get global metrics`() {
        // Given
        val metrics = createHarvestPerformanceMetrics(dataSourceId = null, dataType = null)
        whenever(
            harvestRunService.getAllPerformanceMetrics(
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(metrics, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(get("/internal/runs/metrics"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.dataSourceId").doesNotExist())
            .andExpect(jsonPath("$.dataType").doesNotExist())
            .andExpect(jsonPath("$.totalRuns").value(metrics.totalRuns))
    }

    @Test
    fun `should get metrics with days back parameter`() {
        // Given
        val metrics = createHarvestPerformanceMetrics()
        whenever(
            harvestRunService.getAllPerformanceMetrics(
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(metrics, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(get("/internal/runs/metrics").param("daysBack", "30"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.totalRuns").exists())
    }

    @Test
    fun `should get metrics with date range`() {
        // Given
        val metrics = createHarvestPerformanceMetrics()
        whenever(
            harvestRunService.getAllPerformanceMetrics(
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(metrics, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(
                get("/internal/runs/metrics")
                    .param("startDate", "2024-01-01T00:00:00Z")
                    .param("endDate", "2024-01-31T23:59:59Z"),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.totalRuns").exists())
    }

    @Test
    fun `should return 400 for invalid start date format`() {
        // When/Then
        mockMvc
            .perform(get("/internal/runs/metrics").param("startDate", "invalid-date"))
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.error").exists())
    }

    @Test
    fun `should return 400 for invalid end date format`() {
        // When/Then
        mockMvc
            .perform(get("/internal/runs/metrics").param("endDate", "invalid-date"))
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.error").exists())
    }

    @Test
    fun `should get metrics with limit parameter`() {
        // Given
        val metrics = createHarvestPerformanceMetrics()
        whenever(
            harvestRunService.getAllPerformanceMetrics(
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(metrics, HttpStatus.OK))

        // When/Then
        mockMvc
            .perform(get("/internal/runs/metrics").param("limit", "10"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.totalRuns").exists())
    }

    @Test
    fun `should return 404 when metrics not found`() {
        // Given
        val testDataSourceId = UUID.randomUUID().toString()
        whenever(
            harvestRunService.getPerformanceMetrics(
                eq(testDataSourceId),
                eq("dataset"),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
                anyOrNull(),
            ),
        ).thenReturn(Pair(null, HttpStatus.NOT_FOUND))

        // When/Then
        mockMvc
            .perform(
                get("/internal/runs/metrics")
                    .param("dataSourceId", testDataSourceId)
                    .param("dataType", "dataset"),
            ).andExpect(status().isNotFound)
    }

    private fun createHarvestRunDetails(
        id: Long = 1L,
        dataSourceId: String = UUID.randomUUID().toString(),
        dataType: String = "dataset",
    ): HarvestRunDetails {
        val now = Instant.now()
        return HarvestRunDetails(
            id = id,
            dataSourceId = dataSourceId,
            dataType = dataType,
            runStartedAt = now,
            runEndedAt = null,
            totalDurationMs = null,
            phaseDurations =
                PhaseDurations(
                    initDurationMs = null,
                    harvestDurationMs = null,
                    reasoningDurationMs = null,
                    rdfParsingDurationMs = null,
                    searchProcessingDurationMs = null,
                    aiSearchProcessingDurationMs = null,
                    apiProcessingDurationMs = null,
                    sparqlProcessingDurationMs = null,
                ),
            resourceCounts =
                ResourceCounts(
                    totalResources = null,
                    changedResourcesCount = null,
                    unchangedResourcesCount = null,
                    removedResourcesCount = null,
                ),
            status = "IN_PROGRESS",
            errorMessage = null,
        )
    }

    private fun createHarvestPerformanceMetrics(
        dataSourceId: String? = UUID.randomUUID().toString(),
        dataType: String? = "dataset",
    ): HarvestPerformanceMetrics {
        val now = Instant.now()
        return HarvestPerformanceMetrics(
            dataSourceId = dataSourceId,
            dataType = dataType,
            totalRuns = 10,
            completedRuns = 8,
            failedRuns = 2,
            averageTotalDurationMs = 5000.0,
            averageHarvestDurationMs = 2000.0,
            averageReasoningDurationMs = 1000.0,
            averageRdfParsingDurationMs = 500.0,
            averageSearchProcessingDurationMs = 300.0,
            averageAiSearchProcessingDurationMs = 200.0,
            averageApiProcessingDurationMs = 100.0,
            averageSparqlProcessingDurationMs = 50.0,
            periodStart = now.minusSeconds(86400),
            periodEnd = now,
        )
    }
}
