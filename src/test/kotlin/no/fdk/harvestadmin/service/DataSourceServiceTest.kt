package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.entity.DataSourceEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.exception.ConflictException
import no.fdk.harvestadmin.exception.NotFoundException
import no.fdk.harvestadmin.exception.ValidationException
import no.fdk.harvestadmin.kafka.KafkaHarvestEventPublisher
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.repository.DataSourceRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.Optional
import java.util.UUID

class DataSourceServiceTest {
    private val dataSourceRepository: DataSourceRepository = mock()
    private val harvestRunService: HarvestRunService = mock()
    private val kafkaHarvestEventPublisher: KafkaHarvestEventPublisher = mock()
    private val harvestRunRepository: HarvestRunRepository = mock()
    private val harvestMetricsService: HarvestMetricsService = mock()

    private val service =
        DataSourceService(
            dataSourceRepository = dataSourceRepository,
            harvestRunService = harvestRunService,
            kafkaHarvestEventPublisher = kafkaHarvestEventPublisher,
            harvestRunRepository = harvestRunRepository,
            harvestMetricsService = harvestMetricsService,
            scheduledSlots = 12,
        )

    @Test
    fun `startHarvestingByUrlAndDataType should start harvest when single matching source in org`() {
        val org = "test-org"
        val url = "https://example.com/data"
        val dataType = DataType.DATASET
        val dataSourceId = UUID.randomUUID().toString()

        val dataSourceEntity =
            DataSourceEntity(
                id = dataSourceId,
                publisherId = org,
                dataType = dataType,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = url,
                acceptHeader = "application/rdf+xml",
                description = null,
            )

        whenever(dataSourceRepository.findByUrlAndDataType(url, dataType)).thenReturn(listOf(dataSourceEntity))
        whenever(dataSourceRepository.findById(dataSourceId)).thenReturn(Optional.of(dataSourceEntity))
        whenever(harvestRunRepository.save(any<HarvestRunEntity>())).thenAnswer { it.arguments[0] as HarvestRunEntity }

        service.startHarvestingByUrlAndDataType(
            org = org,
            url = url,
            dataType = dataType,
        )

        verify(harvestRunService).persistEvent(any<HarvestEvent>())
        verify(kafkaHarvestEventPublisher).publishEvent(any<HarvestEvent>())
    }

    @Test
    fun `startHarvestingByUrlAndDataType should throw NotFound when no matching source`() {
        val org = "test-org"
        val url = "https://example.com/data"
        val dataType = DataType.DATASET

        whenever(dataSourceRepository.findByUrlAndDataType(url, dataType)).thenReturn(emptyList())

        assertThrows(NotFoundException::class.java) {
            service.startHarvestingByUrlAndDataType(
                org = org,
                url = url,
                dataType = dataType,
            )
        }
    }

    @Test
    fun `startHarvestingByUrlAndDataType should throw ValidationException when source belongs to other org`() {
        val org = "test-org"
        val otherOrg = "other-org"
        val url = "https://example.com/data"
        val dataType = DataType.DATASET

        val dataSourceEntity =
            DataSourceEntity(
                id = UUID.randomUUID().toString(),
                publisherId = otherOrg,
                dataType = dataType,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = url,
                acceptHeader = "application/rdf+xml",
                description = null,
            )

        whenever(dataSourceRepository.findByUrlAndDataType(url, dataType)).thenReturn(listOf(dataSourceEntity))

        assertThrows(ValidationException::class.java) {
            service.startHarvestingByUrlAndDataType(
                org = org,
                url = url,
                dataType = dataType,
            )
        }
    }

    @Test
    fun `startHarvestingByUrlAndDataType should throw ConflictException when multiple sources in org`() {
        val org = "test-org"
        val url = "https://example.com/data"
        val dataType = DataType.DATASET

        val ds1 =
            DataSourceEntity(
                id = UUID.randomUUID().toString(),
                publisherId = org,
                dataType = dataType,
                dataSourceType = DataSourceType.DCAT_AP_NO,
                url = url,
                acceptHeader = "application/rdf+xml",
                description = null,
            )
        val ds2 = ds1.copy(id = UUID.randomUUID().toString())

        whenever(dataSourceRepository.findByUrlAndDataType(url, dataType)).thenReturn(listOf(ds1, ds2))

        assertThrows(ConflictException::class.java) {
            service.startHarvestingByUrlAndDataType(
                org = org,
                url = url,
                dataType = dataType,
            )
        }
    }
}
