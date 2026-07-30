package no.fdk.harvestadmin.service

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.time.Instant
import java.util.UUID

class HarvestMetricsServiceTest {
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var harvestMetricsService: HarvestMetricsService

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        val harvestRunRepository: HarvestRunRepository = mock()
        harvestMetricsService = HarvestMetricsService(meterRegistry, harvestRunRepository)
    }

    @Test
    fun `recordEventProcessed increments errors counter when errorMessage is set`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.REASONING)
                .setRunId(UUID.randomUUID().toString())
                .setDataSourceId(UUID.randomUUID().toString())
                .setDataType(DataType.dataset)
                .setErrorMessage("boom")
                .build()

        harvestMetricsService.recordEventProcessed(event)

        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest.events.errors",
                    "phase",
                    "REASONING",
                    "datatype",
                    "dataset",
                ).count(),
        )
    }

    @Test
    fun `recordEventProcessed does not increment errors counter when errorMessage is blank`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(UUID.randomUUID().toString())
                .setDataSourceId(UUID.randomUUID().toString())
                .setDataType(DataType.concept)
                .setErrorMessage(null)
                .build()

        harvestMetricsService.recordEventProcessed(event)

        assertEquals(
            0.0,
            meterRegistry.find("harvest.events.errors").counters().sumOf { it.count() },
        )
    }

    @Test
    fun `recordEventProcessingFailed increments counter with phase and datatype tags`() {
        harvestMetricsService.recordEventProcessingFailed("RDF_PARSING", "information_model")

        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest.events.processing.failed",
                    "phase",
                    "RDF_PARSING",
                    "datatype",
                    "informationmodel",
                ).count(),
        )
    }

    @Test
    fun `recordPublishFailed increments counter with phase and datatype tags`() {
        harvestMetricsService.recordPublishFailed("INITIATING", "dataset")

        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest.events.publish.failed",
                    "phase",
                    "INITIATING",
                    "datatype",
                    "dataset",
                ).count(),
        )
    }

    @Test
    fun `recordRunCompleted tags failed runs with failure reason`() {
        val failedRun =
            HarvestRunEntity(
                runId = UUID.randomUUID().toString(),
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "FAILED",
            )

        harvestMetricsService.recordRunCompleted(failedRun, failureReason = "stale_timeout")
        harvestMetricsService.recordRunCompleted(failedRun.copy(runId = UUID.randomUUID().toString()))

        assertEquals(
            1.0,
            meterRegistry.counter("harvest.runs.failed", "reason", "stale_timeout").count(),
        )
        assertEquals(
            1.0,
            meterRegistry.counter("harvest.runs.failed", "reason", "pipeline").count(),
        )
    }
}
