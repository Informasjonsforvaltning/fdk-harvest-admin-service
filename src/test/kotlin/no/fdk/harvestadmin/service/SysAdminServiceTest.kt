package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.kafka.KafkaHarvestEventPublisher
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ResourceToRemove
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever

class SysAdminServiceTest {
    private val kafkaHarvestEventPublisher: KafkaHarvestEventPublisher = mock()
    private val harvestRunRepository: HarvestRunRepository = mock()
    private val harvestMetricsService: HarvestMetricsService = mock()
    private val harvestRunService: HarvestRunService = mock()

    private val service =
        SysAdminService(
            kafkaHarvestEventPublisher = kafkaHarvestEventPublisher,
            harvestRunRepository = harvestRunRepository,
            harvestMetricsService = harvestMetricsService,
            harvestRunService = harvestRunService,
        )

    @Test
    fun `should publish one REMOVING event per resource`() {
        val resources =
            listOf(
                ResourceToRemove(fdkId = "fdk-1", resourceUri = "https://example.com/r/1"),
                ResourceToRemove(fdkId = "fdk-2", resourceUri = "https://example.com/r/2"),
                ResourceToRemove(fdkId = "fdk-3", resourceUri = "https://example.com/r/3"),
            )
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        service.publishRemovingEvents(DataType.DATASET, resources)

        val captor = argumentCaptor<HarvestEvent>()
        verify(kafkaHarvestEventPublisher, times(3)).publishEvent(captor.capture())

        val events = captor.allValues
        assertEquals(3, events.size)

        events.forEachIndexed { idx, event ->
            assertEquals(HarvestPhase.REMOVING, event.phase)
            assertEquals(no.fdk.harvest.DataType.dataset, event.dataType)
            assertEquals(resources[idx].fdkId, event.fdkId.toString())
            assertEquals(resources[idx].resourceUri, event.resourceUri.toString())
            assertEquals("sys-admin-operations", event.dataSourceId.toString())
            assertNotNull(event.startTime)
            assertNotNull(event.endTime)
            assertNull(event.dataSourceUrl)
            assertNull(event.errorMessage)
            assertEquals(false, event.forced)
        }

        val runIds = events.map { it.runId.toString() }.toSet()
        assertEquals(3, runIds.size, "Each resource should have its own runId")
    }

    @Test
    fun `should create harvest run and record metrics per resource`() {
        val resources =
            listOf(
                ResourceToRemove(fdkId = "fdk-1", resourceUri = "https://example.com/r/1"),
                ResourceToRemove(fdkId = "fdk-2", resourceUri = "https://example.com/r/2"),
            )
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        service.publishRemovingEvents(DataType.DATASET, resources)

        val runCaptor = argumentCaptor<HarvestRunEntity>()
        verify(harvestRunRepository, times(2)).save(runCaptor.capture())

        runCaptor.allValues.forEach { run ->
            assertEquals("sys-admin-operations", run.dataSourceId)
            assertEquals("DATASET", run.dataType)
            assertEquals("IN_PROGRESS", run.status)
            assertEquals(false, run.removeAll)
            assertEquals(false, run.forced)
        }

        verify(harvestMetricsService, times(2)).recordRunStarted(any())
        verify(harvestRunService, times(2)).persistEvent(any())
    }

    @Test
    fun `should publish no events when resources list is empty`() {
        service.publishRemovingEvents(DataType.CONCEPT, emptyList())

        verifyNoInteractions(kafkaHarvestEventPublisher)
        verifyNoInteractions(harvestRunRepository)
        verifyNoInteractions(harvestMetricsService)
        verifyNoInteractions(harvestRunService)
    }

    @Test
    fun `should map all data types correctly`() {
        val mapping =
            mapOf(
                DataType.CONCEPT to no.fdk.harvest.DataType.concept,
                DataType.DATASET to no.fdk.harvest.DataType.dataset,
                DataType.INFORMATION_MODEL to no.fdk.harvest.DataType.informationmodel,
                DataType.DATA_SERVICE to no.fdk.harvest.DataType.dataservice,
                DataType.PUBLIC_SERVICE to no.fdk.harvest.DataType.publicService,
                DataType.EVENT to no.fdk.harvest.DataType.event,
            )

        val resource = listOf(ResourceToRemove(fdkId = "fdk-1", resourceUri = "https://example.com/r/1"))
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        mapping.forEach { (input, expected) ->
            val captor = argumentCaptor<HarvestEvent>()

            service.publishRemovingEvents(input, resource)

            verify(kafkaHarvestEventPublisher, times(1)).publishEvent(captor.capture())
            assertEquals(expected, captor.lastValue.dataType, "DataType.$input should map to $expected")

            org.mockito.Mockito.clearInvocations(kafkaHarvestEventPublisher)
            org.mockito.Mockito.clearInvocations(harvestRunRepository)
            org.mockito.Mockito.clearInvocations(harvestMetricsService)
            org.mockito.Mockito.clearInvocations(harvestRunService)
        }
    }

    @Test
    fun `should publish single event for single resource`() {
        val resource = ResourceToRemove(fdkId = "fdk-42", resourceUri = "https://example.com/resource/42")
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        service.publishRemovingEvents(DataType.PUBLIC_SERVICE, listOf(resource))

        val captor = argumentCaptor<HarvestEvent>()
        verify(kafkaHarvestEventPublisher).publishEvent(captor.capture())

        val event = captor.firstValue
        assertEquals(HarvestPhase.REMOVING, event.phase)
        assertEquals(no.fdk.harvest.DataType.publicService, event.dataType)
        assertEquals("fdk-42", event.fdkId.toString())
        assertEquals("https://example.com/resource/42", event.resourceUri.toString())
        assertEquals("sys-admin-operations", event.dataSourceId.toString())
    }
}
