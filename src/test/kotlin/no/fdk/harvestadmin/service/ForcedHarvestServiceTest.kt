package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.client.HarvestedSource
import no.fdk.harvestadmin.client.HarvesterClient
import no.fdk.harvestadmin.client.SourceLookupResponse
import no.fdk.harvestadmin.entity.DataSourceEntity
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ForcedHarvestRun
import no.fdk.harvestadmin.repository.DataSourceRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class ForcedHarvestServiceTest {
    private val harvesterClient: HarvesterClient = mock()
    private val dataSourceRepository: DataSourceRepository = mock()
    private val dataSourceService: DataSourceService = mock()
    private val service = ForcedHarvestService(harvesterClient, dataSourceRepository, dataSourceService)

    @Test
    fun `starts one forced harvest per active data source`() {
        val sourceA = dataSource("ds-a", "https://a.example/catalog")
        val inactiveB = dataSource("ds-b", "https://b.example/catalog", active = false)
        whenever(harvesterClient.lookupSources(listOf("a1", "a2", "b1", "x"))).thenReturn(
            SourceLookupResponse(
                sources =
                listOf(
                    HarvestedSource("https://a.example/catalog", "DATASET", listOf("a1", "a2")),
                    HarvestedSource("https://b.example/catalog", "DATASET", listOf("b1")),
                ),
                notFound = listOf("x"),
            ),
        )
        whenever(dataSourceRepository.findByUrlAndDataType("https://a.example/catalog", DataType.DATASET)).thenReturn(listOf(sourceA))
        whenever(dataSourceRepository.findByUrlAndDataType("https://b.example/catalog", DataType.DATASET)).thenReturn(listOf(inactiveB))

        val response = service.forceHarvest(DataType.DATASET, listOf("a1", "a2", "b1", "x"))

        verify(dataSourceService).startForcedHarvest(sourceA, listOf("a1", "a2"))
        verify(dataSourceService, times(1)).startForcedHarvest(any(), any())
        assertEquals(listOf(ForcedHarvestRun("ds-a", listOf("a1", "a2"))), response.startedRuns)
        assertEquals(listOf("b1", "x"), response.notFound)
    }

    @Test
    fun `matches data source on both url and data type`() {
        val datasetSource = dataSource("ds-dataset", "https://a.example/catalog")
        whenever(harvesterClient.lookupSources(listOf("dataset", "dataservice"))).thenReturn(
            SourceLookupResponse(
                sources =
                listOf(
                    HarvestedSource("https://a.example/catalog", "DATASET", listOf("dataset")),
                    HarvestedSource("https://a.example/catalog", "DATASERVICE", listOf("dataservice")),
                ),
            ),
        )
        whenever(dataSourceRepository.findByUrlAndDataType("https://a.example/catalog", DataType.DATASET)).thenReturn(listOf(datasetSource))

        val response = service.forceHarvest(DataType.DATASET, listOf("dataset", "dataservice"))

        verify(dataSourceService).startForcedHarvest(datasetSource, listOf("dataset"))
        verify(dataSourceService, times(1)).startForcedHarvest(any(), any())
        assertEquals(listOf(ForcedHarvestRun("ds-dataset", listOf("dataset"))), response.startedRuns)
        assertEquals(listOf("dataservice"), response.notFound)
    }

    private fun dataSource(id: String, url: String, active: Boolean = true) = DataSourceEntity(
        id = id,
        publisherId = "org",
        dataType = DataType.DATASET,
        dataSourceType = DataSourceType.DCAT_AP_NO,
        url = url,
        acceptHeader = "text/turtle",
        description = null,
        active = active,
    )
}
