package no.fdk.harvestadmin.service

import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus
import java.time.Instant
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class HarvestRunServiceTest {
    @Mock
    private lateinit var harvestEventRepository: HarvestEventRepository

    @Mock
    private lateinit var harvestRunRepository: HarvestRunRepository

    @Mock
    private lateinit var harvestMetricsService: HarvestMetricsService

    private lateinit var harvestRunService: HarvestRunService

    @BeforeEach
    fun setUp() {
        harvestRunService = HarvestRunService(harvestEventRepository, harvestRunRepository, harvestMetricsService, 30L)
    }

    @Test
    fun `should persist harvest event`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(Instant.now().toString())
                .build()

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "IN_PROGRESS",
            )
        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val eventCaptor = ArgumentCaptor.forClass(HarvestEventEntity::class.java)
        verify(harvestEventRepository).save(eventCaptor.capture())
        val savedEvent = eventCaptor.value
        assertEquals("HARVESTING", savedEvent.eventType)
        assertEquals(dataSourceId, savedEvent.dataSourceId)
        assertEquals(runId, savedEvent.runId)

        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val updatedRun = runCaptor.value
        assertEquals("HARVESTING", updatedRun.currentPhase)
    }

    @Test
    fun `should skip INITIATING events in processor but persist in service`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runId = UUID.randomUUID().toString()
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.INITIATING)
                .setDataSourceId(dataSourceId)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(null)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then - event should still be persisted
        verify(harvestEventRepository).save(any())
        // But run update should be skipped since run doesn't exist
        verify(harvestRunRepository, never()).save(any())
    }

    @Test
    fun `should get current state for data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataType = "dataset"
        val runEntity =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = dataSourceId,
                dataType = dataType,
                runStartedAt = Instant.now(),
                currentPhase = "HARVESTING",
                status = "IN_PROGRESS",
            )
        whenever(harvestRunRepository.findCurrentRun(dataSourceId, dataType)).thenReturn(runEntity)

        // When
        val (states, httpStatus) = harvestRunService.getCurrentState(dataSourceId, dataType)

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertEquals(1, states.size)
        assertEquals(dataSourceId, states[0].dataSourceId)
        assertEquals(dataType, states[0].dataType)
        assertEquals("HARVESTING", states[0].currentPhase)
    }

    @Test
    fun `should return empty list when no current state found`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        whenever(harvestRunRepository.findCurrentRun(dataSourceId, "dataset")).thenReturn(null)

        // When
        val (states, httpStatus) = harvestRunService.getCurrentState(dataSourceId, "dataset")

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertEquals(0, states.size)
    }

    @Test
    fun `should get all in-progress states`() {
        // Given
        val run1 =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "IN_PROGRESS",
            )
        val run2 =
            HarvestRunEntity(
                id = 2L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "concept",
                runStartedAt = Instant.now(),
                status = "IN_PROGRESS",
            )
        whenever(harvestRunRepository.findAllInProgress()).thenReturn(listOf(run1, run2))

        // When
        val runs = harvestRunService.getAllInProgressStates()

        // Then
        assertEquals(2, runs.size)
        assertEquals(run1.runId, runs[0].runId)
        assertEquals(run2.runId, runs[1].runId)
    }

    @Test
    fun `should get performance metrics for data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val dataType = "dataset"
        val runEntity =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = dataSourceId,
                dataType = dataType,
                runStartedAt = Instant.now(),
                runEndedAt = Instant.now().plusSeconds(5),
                totalDurationMs = 5000L,
                harvestDurationMs = 2000L,
                status = "COMPLETED",
            )
        whenever(
            harvestRunRepository.findCompletedRuns(
                eq(dataSourceId),
                eq(dataType),
                any(),
            ),
        ).thenReturn(listOf(runEntity))

        // When
        val (metrics, httpStatus) = harvestRunService.getPerformanceMetrics(dataSourceId, dataType, daysBack = 30)

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertNotNull(metrics)
        assertEquals(dataSourceId, metrics!!.dataSourceId)
        assertEquals(dataType, metrics.dataType)
        assertEquals(1, metrics.totalRuns)
        assertEquals(1, metrics.completedRuns)
        assertEquals(0, metrics.failedRuns)
    }

    @Test
    fun `should return 404 when no metrics found`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        whenever(
            harvestRunRepository.findCompletedRuns(
                eq(dataSourceId),
                eq("dataset"),
                any(),
            ),
        ).thenReturn(emptyList())

        // When
        val (metrics, httpStatus) = harvestRunService.getPerformanceMetrics(dataSourceId, "dataset", daysBack = 30)

        // Then
        assertEquals(HttpStatus.NOT_FOUND, httpStatus)
        assertEquals(null, metrics)
    }

    @Test
    fun `should get performance metrics with limit`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runEntity =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "COMPLETED",
            )
        whenever(
            harvestRunRepository.findLastCompletedRuns(
                eq(dataSourceId),
                eq("dataset"),
                any(),
            ),
        ).thenReturn(listOf(runEntity))

        // When
        val (metrics, httpStatus) = harvestRunService.getPerformanceMetrics(dataSourceId, "dataset", limit = 10)

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertNotNull(metrics)
        assertEquals(1, metrics!!.totalRuns)
    }

    @Test
    fun `should get all performance metrics`() {
        // Given
        val run1 =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "COMPLETED",
            )
        whenever(harvestRunRepository.findAllCompletedRuns(any())).thenReturn(listOf(run1))

        // When
        val (metrics, httpStatus) = harvestRunService.getAllPerformanceMetrics(daysBack = 30)

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertNotNull(metrics)
        assertEquals(null, metrics!!.dataSourceId)
        assertEquals(null, metrics.dataType)
        assertEquals(1, metrics.totalRuns)
    }

    @Test
    fun `should get harvest run by id`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val runEntity =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "COMPLETED",
            )
        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(runEntity)

        // When
        val (run, httpStatus) = harvestRunService.getHarvestRun(runId)

        // Then
        assertEquals(HttpStatus.OK, httpStatus)
        assertNotNull(run)
        assertEquals(runEntity.runId, run!!.runId)
        assertEquals(runEntity.dataSourceId, run.dataSourceId)
    }

    @Test
    fun `should return 404 when harvest run not found`() {
        // Given
        val runId = UUID.randomUUID().toString()
        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(null)

        // When
        val (run, httpStatus) = harvestRunService.getHarvestRun(runId)

        // Then
        assertEquals(HttpStatus.NOT_FOUND, httpStatus)
        assertEquals(null, run)
    }

    @Test
    fun `should get harvest runs for data source`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val run1 =
            HarvestRunEntity(
                id = 1L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = Instant.now(),
                status = "COMPLETED",
            )
        val run2 =
            HarvestRunEntity(
                id = 2L,
                runId = UUID.randomUUID().toString(),
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = Instant.now().minusSeconds(3600),
                status = "COMPLETED",
            )
        whenever(
            harvestRunRepository.findByDataSourceIdAndDataTypeOrderByRunStartedAtDesc(
                dataSourceId,
                "dataset",
            ),
        ).thenReturn(listOf(run1, run2))

        // When
        val runs = harvestRunService.getHarvestRuns(dataSourceId, "dataset", 50)

        // Then
        assertEquals(2, runs.size)
        assertEquals(run1.runId, runs[0].runId)
        assertEquals(run2.runId, runs[1].runId)
    }

    @Test
    fun `should limit harvest runs to specified limit`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runs =
            (1..10).map {
                HarvestRunEntity(
                    id = it.toLong(),
                    runId = UUID.randomUUID().toString(),
                    dataSourceId = dataSourceId,
                    dataType = "dataset",
                    runStartedAt = Instant.now().minusSeconds(it.toLong() * 3600),
                    status = "COMPLETED",
                )
            }
        whenever(
            harvestRunRepository.findByDataSourceIdAndDataTypeOrderByRunStartedAtDesc(
                dataSourceId,
                "dataset",
            ),
        ).thenReturn(runs)

        // When
        val limitedRuns = harvestRunService.getHarvestRuns(dataSourceId, "dataset", 5)

        // Then
        assertEquals(5, limitedRuns.size)
    }
}
