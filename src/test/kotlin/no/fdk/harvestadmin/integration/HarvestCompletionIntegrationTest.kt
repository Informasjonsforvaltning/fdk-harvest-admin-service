package no.fdk.harvestadmin.integration

import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestEventEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestEventRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import no.fdk.harvestadmin.service.HarvestEventIngestionService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant
import java.util.UUID

class HarvestCompletionIntegrationTest : BaseIntegrationTest() {
    @Autowired
    private lateinit var harvestEventRepository: HarvestEventRepository

    @Autowired
    private lateinit var harvestRunRepository: HarvestRunRepository

    @Autowired
    private lateinit var harvestEventIngestionService: HarvestEventIngestionService

    private val baseTime: Instant = Instant.parse("2024-01-01T10:00:00Z")

    private val resourcePhases =
        listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING", "AI_SEARCH_PROCESSING", "SPARQL_PROCESSING")

    @BeforeEach
    fun cleanDatabase() {
        harvestEventRepository.deleteAll()
        harvestRunRepository.deleteAll()
    }

    private fun event(
        runId: String,
        phase: String,
        fdkId: String?,
        createdAt: Instant,
        endTime: Instant? = createdAt,
        errorMessage: String? = null,
    ) = HarvestEventEntity(
        eventType = phase,
        dataSourceId = "ds-1",
        runId = runId,
        dataType = "dataset",
        fdkId = fdkId,
        endTime = endTime?.toString(),
        errorMessage = errorMessage,
        createdAt = createdAt,
    )

    @Test
    fun `countCompletedResourcesPerPhase uses the latest event per resource and filters errors`() {
        val runId = UUID.randomUUID().toString()

        harvestEventRepository.saveAll(
            listOf(
                // Resource A: older event errored, newer event completed -> latest wins -> completed
                event(runId, "REASONING", "A", baseTime, errorMessage = "boom"),
                event(runId, "REASONING", "A", baseTime.plusSeconds(10)),
                // Resource B: single completed event -> completed
                event(runId, "REASONING", "B", baseTime.plusSeconds(5)),
                // Resource C: older completed, newer errored -> latest errored -> NOT completed
                event(runId, "REASONING", "C", baseTime),
                event(runId, "REASONING", "C", baseTime.plusSeconds(10), errorMessage = "boom"),
                // Resource D: latest event has no endTime -> NOT completed
                event(runId, "REASONING", "D", baseTime.plusSeconds(20), endTime = null),
            ),
        )

        val countsByPhase =
            harvestEventRepository
                .countCompletedResourcesPerPhase(runId, resourcePhases)
                .associate { (phase, count) -> phase as String to (count as Number).toInt() }

        // Only A and B have a completed latest event
        assertEquals(2, countsByPhase["REASONING"])
        // Phases with no events are absent from the result
        assertEquals(null, countsByPhase["RDF_PARSING"])
    }

    @Test
    fun `countResourcesCompletedInAllPhases counts only resources completed in every phase`() {
        val runId = UUID.randomUUID().toString()
        val phases = listOf("REASONING", "RDF_PARSING", "RESOURCE_PROCESSING", "SEARCH_PROCESSING")

        harvestEventRepository.saveAll(
            buildList {
                // Resource A completed in all four phases
                phases.forEach { add(event(runId, it, "A", baseTime)) }
                // Resource B completed in only three phases (missing SEARCH_PROCESSING)
                phases.dropLast(1).forEach { add(event(runId, it, "B", baseTime)) }
                // Resource C has events in all four phases but SEARCH_PROCESSING has no endTime
                phases.dropLast(1).forEach { add(event(runId, it, "C", baseTime)) }
                add(event(runId, "SEARCH_PROCESSING", "C", baseTime, endTime = null))
            },
        )

        val count =
            harvestEventRepository.countResourcesCompletedInAllPhases(runId, phases, phases.size.toLong())

        // Only resource A is completed in all four phases
        assertEquals(1L, count)
    }

    @Test
    fun `persistEvent marks run COMPLETED when required phases are done and optional phases are unused`() {
        val runId = UUID.randomUUID().toString()
        val fdkId = "resource-1"

        harvestRunRepository.save(
            HarvestRunEntity(
                runId = runId,
                dataSourceId = "ds-1",
                dataType = "dataset",
                runStartedAt = baseTime,
                status = "IN_PROGRESS",
                changedResourcesCount = 1,
                removedResourcesCount = 0,
            ),
        )

        // HARVESTING completed, plus required resource phases completed for the single resource.
        // No AI_SEARCH_PROCESSING / SPARQL_PROCESSING events -> those optional phases must not block.
        harvestEventRepository.saveAll(
            listOf(
                event(runId, "HARVESTING", null, baseTime),
                event(runId, "REASONING", fdkId, baseTime.plusSeconds(1)),
                event(runId, "RDF_PARSING", fdkId, baseTime.plusSeconds(2)),
                event(runId, "RESOURCE_PROCESSING", fdkId, baseTime.plusSeconds(3)),
            ),
        )

        // Final required phase arrives through the service, triggering completion evaluation.
        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SEARCH_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId(fdkId)
                .setStartTime(baseTime.plusSeconds(4).toString())
                .setEndTime(baseTime.plusSeconds(5).toString())
                .build()

        harvestEventIngestionService.persistEvent(finalEvent)

        val updatedRun = harvestRunRepository.findByRunId(runId)
        assertEquals("COMPLETED", updatedRun?.status)
    }

    @Test
    fun `persistEvent keeps run IN_PROGRESS when a required phase is incomplete`() {
        val runId = UUID.randomUUID().toString()
        val fdkId = "resource-1"

        harvestRunRepository.save(
            HarvestRunEntity(
                runId = runId,
                dataSourceId = "ds-1",
                dataType = "dataset",
                runStartedAt = baseTime,
                status = "IN_PROGRESS",
                // Two resources expected, but only one will be completed
                changedResourcesCount = 2,
                removedResourcesCount = 0,
            ),
        )

        harvestEventRepository.saveAll(
            listOf(
                event(runId, "HARVESTING", null, baseTime),
                event(runId, "REASONING", fdkId, baseTime.plusSeconds(1)),
                event(runId, "RDF_PARSING", fdkId, baseTime.plusSeconds(2)),
                event(runId, "RESOURCE_PROCESSING", fdkId, baseTime.plusSeconds(3)),
            ),
        )

        val finalEvent =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.SEARCH_PROCESSING)
                .setRunId(runId)
                .setDataType(DataType.dataset)
                .setFdkId(fdkId)
                .setStartTime(baseTime.plusSeconds(4).toString())
                .setEndTime(baseTime.plusSeconds(5).toString())
                .build()

        harvestEventIngestionService.persistEvent(finalEvent)

        val updatedRun = harvestRunRepository.findByRunId(runId)
        assertEquals("IN_PROGRESS", updatedRun?.status)
    }
}
