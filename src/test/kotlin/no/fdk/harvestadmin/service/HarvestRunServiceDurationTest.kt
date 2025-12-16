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
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class HarvestRunServiceDurationTest {
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
    fun `should calculate init duration correctly`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val runStartedAt = baseTime
        val harvestingStartTime = baseTime.plusSeconds(100) // 100 seconds after run start

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = runStartedAt,
                status = "IN_PROGRESS",
            )

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(harvestingStartTime.toString())
                .setEndTime(harvestingStartTime.plusSeconds(200).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedInitDuration = ChronoUnit.MILLIS.between(runStartedAt, harvestingStartTime)
        assertEquals(expectedInitDuration, savedRun.initDurationMs)
        assertEquals(100_000L, savedRun.initDurationMs) // 100 seconds = 100,000 ms
    }

    @Test
    fun `should calculate harvest duration correctly`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val harvestingStart = baseTime.plusSeconds(100)
        val harvestingEnd = harvestingStart.plusSeconds(200)

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                status = "IN_PROGRESS",
            )

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(harvestingStart.toString())
                .setEndTime(harvestingEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedHarvestDuration = ChronoUnit.MILLIS.between(harvestingStart, harvestingEnd)
        assertEquals(expectedHarvestDuration, savedRun.harvestDurationMs)
        assertEquals(200_000L, savedRun.harvestDurationMs) // 200 seconds = 200,000 ms
    }

    @Test
    fun `should accumulate reasoning duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                reasoningDurationMs = 50_000L, // Already has 50 seconds
                status = "IN_PROGRESS",
            )

        val event1Start = baseTime.plusSeconds(1000)
        val event1End = event1Start.plusSeconds(30) // 30 seconds

        val event1 =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.REASONING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(event1Start.toString())
                .setEndTime(event1End.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event1)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 50_000L + 30_000L // Existing 50s + new 30s
        assertEquals(expectedDuration, savedRun.reasoningDurationMs)
        assertEquals(80_000L, savedRun.reasoningDurationMs)
    }

    @Test
    fun `should accumulate RDF parsing duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                rdfParsingDurationMs = 25_000L, // Already has 25 seconds
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart.plusSeconds(15) // 15 seconds

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.RDF_PARSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 25_000L + 15_000L // Existing 25s + new 15s
        assertEquals(expectedDuration, savedRun.rdfParsingDurationMs)
        assertEquals(40_000L, savedRun.rdfParsingDurationMs)
    }

    @Test
    fun `should accumulate search processing duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                searchProcessingDurationMs = 10_000L, // Already has 10 seconds
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart.plusSeconds(20) // 20 seconds

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SEARCH_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 10_000L + 20_000L // Existing 10s + new 20s
        assertEquals(expectedDuration, savedRun.searchProcessingDurationMs)
        assertEquals(30_000L, savedRun.searchProcessingDurationMs)
    }

    @Test
    fun `should accumulate AI search processing duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                aiSearchProcessingDurationMs = 5_000L, // Already has 5 seconds
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart.plusSeconds(12) // 12 seconds

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.AI_SEARCH_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 5_000L + 12_000L // Existing 5s + new 12s
        assertEquals(expectedDuration, savedRun.aiSearchProcessingDurationMs)
        assertEquals(17_000L, savedRun.aiSearchProcessingDurationMs)
    }

    @Test
    fun `should accumulate resource processing duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                apiProcessingDurationMs = 100_000L, // Already has 100 seconds
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart.plusSeconds(50) // 50 seconds

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.RESOURCE_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 100_000L + 50_000L // Existing 100s + new 50s
        assertEquals(expectedDuration, savedRun.apiProcessingDurationMs)
        assertEquals(150_000L, savedRun.apiProcessingDurationMs)
    }

    @Test
    fun `should accumulate SPARQL processing duration for multiple events`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                sparqlProcessingDurationMs = 8_000L, // Already has 8 seconds
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart.plusSeconds(7) // 7 seconds

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        val expectedDuration = 8_000L + 7_000L // Existing 8s + new 7s
        assertEquals(expectedDuration, savedRun.sparqlProcessingDurationMs)
        assertEquals(15_000L, savedRun.sparqlProcessingDurationMs)
    }

    @Test
    fun `should calculate total duration as sum of all phase durations`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val runStartedAt = baseTime
        val harvestingStart = baseTime.plusSeconds(100)
        val harvestingEnd = harvestingStart.plusSeconds(200)

        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = runStartedAt,
                initDurationMs = 100_000L, // 100 seconds
                harvestDurationMs = 200_000L, // 200 seconds
                reasoningDurationMs = 150_000L, // 150 seconds
                rdfParsingDurationMs = 80_000L, // 80 seconds
                searchProcessingDurationMs = 60_000L, // 60 seconds
                aiSearchProcessingDurationMs = 40_000L, // 40 seconds
                apiProcessingDurationMs = 120_000L, // 120 seconds
                sparqlProcessingDurationMs = 0L, // 0 seconds (will be set by the event)
                status = "IN_PROGRESS",
            )

        // Mock all required phases as complete (return at least one event for each phase)
        val mockEvent =
            HarvestEventEntity(
                id = 1L,
                eventType = "HARVESTING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                endTime = baseTime.plusSeconds(300).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("HARVESTING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("REASONING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RDF_PARSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("AI_SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RESOURCE_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        // SPARQL_PROCESSING is the current phase, so it will be checked from the event itself
        // Mock findByRunIdAndEventType for SPARQL_PROCESSING since getLatestEventForResource will call it
        // when the event has no fdkId or resourceUri
        val sparqlEvent =
            HarvestEventEntity(
                id = 2L,
                eventType = "SPARQL_PROCESSING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                startTime = harvestingEnd.toString(),
                endTime = harvestingEnd.plusSeconds(30).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventType(eq(runId), eq("SPARQL_PROCESSING")),
        ).thenReturn(listOf(sparqlEvent))

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(harvestingEnd.toString())
                .setEndTime(harvestingEnd.plusSeconds(30).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        // Total should be sum of all durations: 100 + 200 + 150 + 80 + 60 + 40 + 120 + 30 = 780 seconds
        // The event adds 30 seconds to sparqlProcessingDurationMs (0 + 30 = 30)
        val expectedTotal = 100_000L + 200_000L + 150_000L + 80_000L + 60_000L + 40_000L + 120_000L + 30_000L
        assertEquals(expectedTotal, savedRun.totalDurationMs)
        assertEquals(780_000L, savedRun.totalDurationMs)
    }

    @Test
    fun `should handle null durations correctly in total calculation`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                initDurationMs = 100_000L,
                harvestDurationMs = 200_000L,
                reasoningDurationMs = null, // Null duration
                rdfParsingDurationMs = 80_000L,
                searchProcessingDurationMs = null, // Null duration
                aiSearchProcessingDurationMs = 40_000L,
                apiProcessingDurationMs = null, // Null duration
                sparqlProcessingDurationMs = 0L, // 0 seconds (will be set by the event)
                status = "IN_PROGRESS",
            )

        // Mock all required phases as complete (return at least one event for each phase)
        val mockEvent =
            HarvestEventEntity(
                id = 1L,
                eventType = "HARVESTING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                endTime = baseTime.plusSeconds(300).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("HARVESTING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("REASONING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RDF_PARSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("AI_SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RESOURCE_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        // SPARQL_PROCESSING is the current phase, so it will be checked from the event itself
        // Mock findByRunIdAndEventType for SPARQL_PROCESSING since getLatestEventForResource will call it
        // when the event has no fdkId or resourceUri
        val sparqlEvent =
            HarvestEventEntity(
                id = 2L,
                eventType = "SPARQL_PROCESSING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                startTime = baseTime.plusSeconds(1000).toString(),
                endTime = baseTime.plusSeconds(1030).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventType(eq(runId), eq("SPARQL_PROCESSING")),
        ).thenReturn(listOf(sparqlEvent))

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(baseTime.plusSeconds(1000).toString())
                .setEndTime(baseTime.plusSeconds(1030).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        // Total should treat null durations as 0: 100 + 200 + 0 + 80 + 0 + 40 + 0 + 30 = 450 seconds
        // The event adds 30 seconds to sparqlProcessingDurationMs (0 + 30 = 30)
        val expectedTotal = 100_000L + 200_000L + 0L + 80_000L + 0L + 40_000L + 0L + 30_000L
        assertEquals(expectedTotal, savedRun.totalDurationMs)
        assertEquals(450_000L, savedRun.totalDurationMs)
    }

    @Test
    fun `should return null total duration when all durations are zero or null`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                initDurationMs = null,
                harvestDurationMs = null,
                reasoningDurationMs = null,
                rdfParsingDurationMs = null,
                searchProcessingDurationMs = null,
                aiSearchProcessingDurationMs = null,
                apiProcessingDurationMs = null,
                sparqlProcessingDurationMs = null,
                status = "IN_PROGRESS",
            )

        // Mock all required phases as complete (return at least one event for each phase)
        val mockEvent =
            HarvestEventEntity(
                id = 1L,
                eventType = "HARVESTING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                endTime = baseTime.plusSeconds(300).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("HARVESTING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("REASONING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RDF_PARSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("AI_SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RESOURCE_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        // SPARQL_PROCESSING is the current phase, so it will be checked from the event itself

        // Use an event with zero duration to test null total
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(baseTime.plusSeconds(1000).toString())
                .setEndTime(baseTime.plusSeconds(1000).toString()) // Same time = 0 duration
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        // Total should be null when sum is 0 (all durations are null/0, and event adds 0)
        assertNull(savedRun.totalDurationMs)
    }

    @Test
    fun `should calculate runEndedAt based on totalDuration and runStartedAt`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val runStartedAt = baseTime
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = runStartedAt,
                initDurationMs = 100_000L,
                harvestDurationMs = 200_000L,
                reasoningDurationMs = 150_000L,
                rdfParsingDurationMs = 80_000L,
                searchProcessingDurationMs = 60_000L,
                aiSearchProcessingDurationMs = 40_000L,
                apiProcessingDurationMs = 120_000L,
                sparqlProcessingDurationMs = 0L, // 0 seconds (will be set by the event)
                status = "IN_PROGRESS",
            )

        // Mock all required phases as complete (return at least one event for each phase)
        val mockEvent =
            HarvestEventEntity(
                id = 1L,
                eventType = "HARVESTING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                endTime = baseTime.plusSeconds(300).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("HARVESTING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("REASONING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RDF_PARSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("AI_SEARCH_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        whenever(
            harvestEventRepository.findByRunIdAndEventTypeAndEndTimeIsNotNull(eq(runId), eq("RESOURCE_PROCESSING")),
        ).thenReturn(listOf(mockEvent))
        // SPARQL_PROCESSING is the current phase, so it will be checked from the event itself
        // Mock findByRunIdAndEventType for SPARQL_PROCESSING since getLatestEventForResource will call it
        // when the event has no fdkId or resourceUri
        val sparqlEvent =
            HarvestEventEntity(
                id = 2L,
                eventType = "SPARQL_PROCESSING",
                dataSourceId = existingRun.dataSourceId,
                runId = runId,
                dataType = existingRun.dataType,
                startTime = baseTime.plusSeconds(1000).toString(),
                endTime = baseTime.plusSeconds(1030).toString(),
            )
        whenever(
            harvestEventRepository.findByRunIdAndEventType(eq(runId), eq("SPARQL_PROCESSING")),
        ).thenReturn(listOf(sparqlEvent))

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SPARQL_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(baseTime.plusSeconds(1000).toString())
                .setEndTime(baseTime.plusSeconds(1030).toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        // Total should be sum of all durations: 100 + 200 + 150 + 80 + 60 + 40 + 120 + 30 = 780 seconds
        // The event adds 30 seconds to sparqlProcessingDurationMs (0 + 30 = 30)
        val expectedTotalDuration = 780_000L // 780 seconds in milliseconds
        val expectedRunEndedAt = runStartedAt.plusMillis(expectedTotalDuration)

        assertEquals(expectedTotalDuration, savedRun.totalDurationMs)
        assertNotNull(savedRun.runEndedAt)
        assertEquals(expectedRunEndedAt, savedRun.runEndedAt)
    }

    @Test
    fun `should calculate average durations correctly`() {
        // Given
        val dataSourceId = UUID.randomUUID().toString()
        val runs =
            listOf(
                HarvestRunEntity(
                    id = 1L,
                    runId = UUID.randomUUID().toString(),
                    dataSourceId = dataSourceId,
                    dataType = "dataset",
                    runStartedAt = baseTime,
                    totalDurationMs = 1000_000L, // 1000 seconds
                    harvestDurationMs = 500_000L, // 500 seconds
                    reasoningDurationMs = 200_000L, // 200 seconds
                    rdfParsingDurationMs = 100_000L, // 100 seconds
                    searchProcessingDurationMs = 80_000L, // 80 seconds
                    aiSearchProcessingDurationMs = 60_000L, // 60 seconds
                    apiProcessingDurationMs = 40_000L, // 40 seconds
                    sparqlProcessingDurationMs = 20_000L, // 20 seconds
                    status = "COMPLETED",
                    errorMessage = null,
                ),
                HarvestRunEntity(
                    id = 2L,
                    runId = UUID.randomUUID().toString(),
                    dataSourceId = dataSourceId,
                    dataType = "dataset",
                    runStartedAt = baseTime.plusSeconds(3600),
                    totalDurationMs = 2000_000L, // 2000 seconds
                    harvestDurationMs = 1000_000L, // 1000 seconds
                    reasoningDurationMs = 400_000L, // 400 seconds
                    rdfParsingDurationMs = 200_000L, // 200 seconds
                    searchProcessingDurationMs = 160_000L, // 160 seconds
                    aiSearchProcessingDurationMs = 120_000L, // 120 seconds
                    apiProcessingDurationMs = 80_000L, // 80 seconds
                    sparqlProcessingDurationMs = 40_000L, // 40 seconds
                    status = "COMPLETED",
                    errorMessage = null,
                ),
            )

        whenever(
            harvestRunRepository.findCompletedRuns(
                eq(dataSourceId),
                eq("dataset"),
                any(),
            ),
        ).thenReturn(runs)

        // When
        val (metrics, httpStatus) = harvestRunService.getPerformanceMetrics(dataSourceId, "dataset", daysBack = 30)

        // Then
        assertEquals(org.springframework.http.HttpStatus.OK, httpStatus)
        assertNotNull(metrics)

        // Average total: (1000 + 2000) / 2 = 1500 seconds
        assertEquals(1500_000.0, metrics!!.averageTotalDurationMs)

        // Average harvest: (500 + 1000) / 2 = 750 seconds
        assertEquals(750_000.0, metrics.averageHarvestDurationMs)

        // Average reasoning: (200 + 400) / 2 = 300 seconds
        assertEquals(300_000.0, metrics.averageReasoningDurationMs)

        // Average RDF parsing: (100 + 200) / 2 = 150 seconds
        assertEquals(150_000.0, metrics.averageRdfParsingDurationMs)

        // Average search processing: (80 + 160) / 2 = 120 seconds
        assertEquals(120_000.0, metrics.averageSearchProcessingDurationMs)

        // Average AI search processing: (60 + 120) / 2 = 90 seconds
        assertEquals(90_000.0, metrics.averageAiSearchProcessingDurationMs)

        // Average API processing: (40 + 80) / 2 = 60 seconds
        assertEquals(60_000.0, metrics.averageApiProcessingDurationMs)

        // Average SPARQL processing: (20 + 40) / 2 = 30 seconds
        assertEquals(30_000.0, metrics.averageSparqlProcessingDurationMs)
    }

    @Test
    fun `should handle zero duration correctly`() {
        // Given
        val runId = UUID.randomUUID().toString()
        val existingRun =
            HarvestRunEntity(
                id = 1L,
                runId = runId,
                dataSourceId = UUID.randomUUID().toString(),
                dataType = "dataset",
                runStartedAt = baseTime,
                reasoningDurationMs = 0L, // Zero duration
                status = "IN_PROGRESS",
            )

        val eventStart = baseTime.plusSeconds(1000)
        val eventEnd = eventStart // Same time = 0 duration

        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.REASONING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setStartTime(eventStart.toString())
                .setEndTime(eventEnd.toString())
                .build()

        whenever(harvestRunRepository.findByRunId(runId)).thenReturn(existingRun)
        whenever(harvestEventRepository.save(any())).thenAnswer { it.arguments[0] as HarvestEventEntity }
        whenever(harvestRunRepository.save(any())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        // When
        harvestRunService.persistEvent(event)

        // Then
        val runCaptor = ArgumentCaptor.forClass(HarvestRunEntity::class.java)
        org.mockito.kotlin
            .verify(harvestRunRepository)
            .save(runCaptor.capture())
        val savedRun = runCaptor.value

        // Should still accumulate: 0 + 0 = 0
        assertEquals(0L, savedRun.reasoningDurationMs)
    }
}
