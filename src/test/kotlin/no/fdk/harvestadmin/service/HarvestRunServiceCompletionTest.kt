package no.fdk.harvestadmin.service

import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class HarvestRunServiceCompletionTest {
    @Mock
    private lateinit var harvestEventRepository: HarvestEventRepository

    @Mock
    private lateinit var harvestRunRepository: HarvestRunRepository

    @Mock
    private lateinit var harvestMetricsService: HarvestMetricsService

    private lateinit var ingestionService: HarvestEventIngestionService

    private lateinit var baseTime: Instant

    private val resourcePhases =
        listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")

    @BeforeEach
    fun setUp() {
        val completionEvaluator = HarvestCompletionEvaluator(harvestEventRepository)
        ingestionService =
            HarvestEventIngestionService(
                harvestEventRepository,
                harvestRunRepository,
                harvestMetricsService,
                completionEvaluator,
            )
        baseTime = Instant.parse("2024-01-01T10:00:00Z")
    }

    /** Stubs the grouped total-events-per-phase query so every phase is treated as "used". */
    private fun stubPhaseEventCounts(runId: String, count: Long) {
        whenever(harvestEventRepository.countEventsByPhase(eq(runId))).thenReturn(
            (listOf("INITIATING", "HARVESTING") + resourcePhases).map { arrayOf<Any>(it, count) },
        )
    }

    /** Stubs the completed-resources-per-phase aggregate, the same completed count for every resource phase. */
    private fun stubCompletedResourcesPerPhase(runId: String, completedPerPhase: Long) {
        whenever(harvestEventRepository.countCompletedResourcesPerPhase(eq(runId), any())).thenReturn(
            resourcePhases.map { arrayOf<Any>(it, completedPerPhase) },
        )
    }

    @Test
    fun `should mark run as COMPLETED when all phases have correct event counts matching changed plus removed`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        val expectedCount = (changedCount + removedCount).toLong() // 15

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        // HARVESTING phase (no resource identifiers) - just needs at least one completed event
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Every resource phase has exactly expectedCount completed resources
        stubPhaseEventCounts(runId, expectedCount)
        stubCompletedResourcesPerPhase(runId, expectedCount)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        assertEquals("COMPLETED", savedRun.status)
        assertEquals(changedCount, savedRun.changedResourcesCount)
        assertEquals(removedCount, savedRun.removedResourcesCount)
    }

    @Test
    fun `should NOT mark run as COMPLETED when event counts are less than changed plus removed`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        val actualCompleted = 12L // Less than expected 15

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        stubPhaseEventCounts(runId, actualCompleted)
        stubCompletedResourcesPerPhase(runId, actualCompleted)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$actualCompleted")
                .setStartTime(baseTime.plusSeconds(actualCompleted).toString())
                .setEndTime(baseTime.plusSeconds(actualCompleted + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        assertEquals("IN_PROGRESS", savedRun.status) // Should still be IN_PROGRESS
    }

    @Test
    fun `should NOT mark run as COMPLETED when events have errorMessage`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        // 3 of the 15 resources have an error on their latest event, so SQL reports 12 completed.
        val completedAfterErrors = 12L

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        stubPhaseEventCounts(runId, 15L)
        stubCompletedResourcesPerPhase(runId, completedAfterErrors)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-15")
                .setStartTime(baseTime.plusSeconds(15).toString())
                .setEndTime(baseTime.plusSeconds(16).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        // Only 12 completed resources (errored ones excluded by the SQL aggregate), not 15
        assertEquals("IN_PROGRESS", savedRun.status)
    }

    @Test
    fun `should handle HARVESTING phase separately as it has no resource identifiers`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        val expectedCount = (changedCount + removedCount).toLong()

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        // HARVESTING just needs at least one completed event (no resource count check)
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        stubPhaseEventCounts(runId, expectedCount)
        stubCompletedResourcesPerPhase(runId, expectedCount)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        assertEquals("COMPLETED", savedRun.status)
    }

    @Test
    fun `should use latest event per resource when handling duplicates`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 5
        val removedCount = 2
        val expectedCount = (changedCount + removedCount).toLong() // 7

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Each resource has duplicate REASONING events; the SQL aggregate keeps the latest (no error),
        // so all expectedCount resources count as completed across every phase.
        stubPhaseEventCounts(runId, expectedCount * 2)
        stubCompletedResourcesPerPhase(runId, expectedCount)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        // Should be COMPLETED because latest events (without errors) are used
        assertEquals("COMPLETED", savedRun.status)
    }

    @Test
    fun `should NOT mark run as COMPLETED when HARVESTING phase has no completed events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        val expectedCount = (changedCount + removedCount).toLong()

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = changedCount,
                removedResourcesCount = removedCount,
                status = "IN_PROGRESS",
            )

        // HARVESTING has no completed events
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(0L)

        // All resource phases are complete, but HARVESTING blocks completion
        stubPhaseEventCounts(runId, expectedCount)
        stubCompletedResourcesPerPhase(runId, expectedCount)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        assertEquals("IN_PROGRESS", savedRun.status) // Should still be IN_PROGRESS
    }

    @Test
    fun `should handle case when resource counts are not yet available`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = dataSourceId,
                dataType = "dataset",
                runStartedAt = baseTime,
                changedResourcesCount = null,
                removedResourcesCount = null,
                status = "IN_PROGRESS",
            )

        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // At least one completed resource per phase; no expected count is known
        stubPhaseEventCounts(runId, 1L)
        stubCompletedResourcesPerPhase(runId, 1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-1")
                .setStartTime(baseTime.plusSeconds(1).toString())
                .setEndTime(baseTime.plusSeconds(2).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any<HarvestEventEntity>())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        ingestionService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        // When resource counts are not available, completion only requires at least one completed
        // event per phase, so the run is COMPLETED.
        assertEquals("COMPLETED", savedRun.status)
    }
}
