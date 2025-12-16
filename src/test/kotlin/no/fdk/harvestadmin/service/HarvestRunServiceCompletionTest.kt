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
import org.mockito.Mockito
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

    private lateinit var harvestRunService: HarvestRunService

    private lateinit var baseTime: Instant

    @BeforeEach
    fun setUp() {
        harvestRunService = HarvestRunService(harvestEventRepository, harvestRunRepository, harvestMetricsService, 30L)
        baseTime = Instant.parse("2024-01-01T10:00:00Z")
    }

    @Test
    fun `should mark run as COMPLETED when all phases have correct event counts matching changed plus removed`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5
        val expectedCount = changedCount + removedCount // 15

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

        // Mock HARVESTING phase (no resource identifiers) - just needs at least one event
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock resource-processing phases - need exactly expectedCount events
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")

        // Create expectedCount events for each resource phase
        resourcePhases.forEach { phase ->
            val events =
                (1..expectedCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            whenever(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            // Also mock countByRunIdAndEventType for phase event counts calculation
            whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(expectedCount.toLong())
        }

        // Mock INITIATING phase count
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        // Mock HARVESTING phase count
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        // Mock the final event (SPARQL_PROCESSING)
        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

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
        val expectedCount = changedCount + removedCount // 15
        val actualEventCount = 12 // Less than expected

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

        // Mock HARVESTING phase
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock resource-processing phases - only actualEventCount events (less than expected)
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")

        resourcePhases.forEach { phase ->
            val events =
                (1..actualEventCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            // Use lenient stubbing since some phases may not be checked if completion fails early
            Mockito.lenient().`when`(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            Mockito
                .lenient()
                .`when`(
                    harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase)),
                ).thenReturn(actualEventCount.toLong())
        }

        // Mock INITIATING and HARVESTING phase counts
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$actualEventCount")
                .setStartTime(baseTime.plusSeconds(actualEventCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(actualEventCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

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
        val expectedCount = changedCount + removedCount // 15

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

        // Mock HARVESTING phase
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock resource-processing phases - some events have errorMessage
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")

        resourcePhases.forEach { phase ->
            val events =
                (1..expectedCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        // First 3 events have errors, rest don't
                        errorMessage = if (i <= 3) "Error processing resource" else null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            // Use lenient stubbing since some phases may not be checked if completion fails early
            Mockito.lenient().`when`(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            Mockito
                .lenient()
                .`when`(
                    harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase)),
                ).thenReturn(expectedCount.toLong())
        }

        // Mock INITIATING and HARVESTING phase counts
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        // Should not be COMPLETED because events with errorMessage are filtered out
        // So we only have 12 completed events (15 - 3), not 15
        assertEquals("IN_PROGRESS", savedRun.status)
    }

    @Test
    fun `should handle HARVESTING phase separately as it has no resource identifiers`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val dataSourceId = UUID.randomUUID().toString()
        val changedCount = 10
        val removedCount = 5

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

        // Mock HARVESTING phase - just needs at least one event (no resource count check)
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock all other phases as complete
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")
        val expectedCount = changedCount + removedCount

        resourcePhases.forEach { phase ->
            val events =
                (1..expectedCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            whenever(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(expectedCount.toLong())
        }

        // Mock INITIATING and HARVESTING phase counts
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

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
        val expectedCount = changedCount + removedCount // 7

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

        // Mock HARVESTING phase
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock REASONING phase with duplicates - each resource has 2 events, latest one has endTime and no error
        val reasoningEvents =
            (1..expectedCount).flatMap { i ->
                listOf(
                    // First event (older) - has errorMessage
                    HarvestEventEntity(
                        id = i.toLong() * 10,
                        eventType = "REASONING",
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = "Old error",
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    ),
                    // Second event (newer) - no errorMessage
                    HarvestEventEntity(
                        id = i.toLong() * 10 + 1,
                        eventType = "REASONING",
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong() + 100).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong() + 100),
                    ),
                )
            }
        whenever(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq("REASONING"))).thenReturn(reasoningEvents)
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("REASONING"))).thenReturn((expectedCount * 2).toLong())

        // Mock other phases as complete
        val otherPhases = listOf("RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")
        otherPhases.forEach { phase ->
            val events =
                (1..expectedCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            whenever(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(expectedCount.toLong())
        }

        // Mock INITIATING and HARVESTING phase counts
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

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
        val expectedCount = changedCount + removedCount

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

        // Mock HARVESTING phase - no completed events
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(0L)

        // Mock all other phases as complete
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")
        resourcePhases.forEach { phase ->
            val events =
                (1..expectedCount).map { i ->
                    HarvestEventEntity(
                        id = i.toLong(),
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-$i",
                        endTime = baseTime.plusSeconds(i.toLong()).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(i.toLong()),
                    )
                }
            // Use lenient stubbing since HARVESTING fails first, so these phases won't be checked
            Mockito.lenient().`when`(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            Mockito
                .lenient()
                .`when`(
                    harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase)),
                ).thenReturn(expectedCount.toLong())
        }

        // Mock INITIATING and HARVESTING phase counts
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        Mockito.lenient().`when`(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId("resource-$expectedCount")
                .setStartTime(baseTime.plusSeconds(expectedCount.toLong()).toString())
                .setEndTime(baseTime.plusSeconds(expectedCount.toLong() + 1).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

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

        // Mock HARVESTING phase
        whenever(
            harvestEventRepository.countByRunIdAndEventTypeAndEndTimeIsNotNullAndErrorMessageIsNull(
                eq(runId),
                eq("HARVESTING"),
            ),
        ).thenReturn(1L)

        // Mock resource-processing phases - at least one event
        val resourcePhases =
            listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")
        resourcePhases.forEach { phase ->
            val events =
                listOf(
                    HarvestEventEntity(
                        id = 1L,
                        eventType = phase,
                        dataSourceId = dataSourceId,
                        runId = runId,
                        dataType = "dataset",
                        fdkId = "resource-1",
                        endTime = baseTime.plusSeconds(1).toString(),
                        errorMessage = null,
                        createdAt = baseTime.plusSeconds(1),
                    ),
                )
            whenever(harvestEventRepository.findByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(events)
            whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq(phase))).thenReturn(1L)
        }

        // Mock INITIATING and HARVESTING phase counts
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("INITIATING"))).thenReturn(1L)
        whenever(harvestEventRepository.countByRunIdAndEventType(eq(runId), eq("HARVESTING"))).thenReturn(1L)

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
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(finalEvent)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        verify(harvestRunRepository).save(runCaptor.capture())
        val savedRun = runCaptor.value
        // When resource counts are not available, it should check that there's at least one completed event
        // Since we have events for all phases, it should be COMPLETED
        assertEquals("COMPLETED", savedRun.status)
    }
}
