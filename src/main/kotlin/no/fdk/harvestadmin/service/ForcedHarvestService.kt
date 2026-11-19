package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.client.HarvesterClient
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ForceHarvestResponse
import no.fdk.harvestadmin.model.ForcedHarvestRun
import no.fdk.harvestadmin.repository.DataSourceRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class ForcedHarvestService(
    private val harvesterClient: HarvesterClient,
    private val dataSourceRepository: DataSourceRepository,
    private val dataSourceService: DataSourceService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun forceHarvest(dataType: DataType, fdkIds: Collection<String>): ForceHarvestResponse {
        val lookup = harvesterClient.lookupSources(fdkIds)
        val notFound = lookup.notFound.toMutableList()
        val startedRuns = mutableListOf<ForcedHarvestRun>()

        lookup.sources.forEach { source ->
            val dataSource = dataSourceRepository.findByUrlAndDataType(source.dataSourceUrl, dataType).singleOrNull { it.active }
            if (dataSource == null) {
                logger.warn("No single active ${dataType.value} data source for ${source.dataSourceUrl}, skipping ${source.fdkIds}")
                notFound.addAll(source.fdkIds)
            } else {
                dataSourceService.startForcedHarvest(dataSource, source.fdkIds)
                startedRuns.add(ForcedHarvestRun(dataSourceId = dataSource.id, fdkIds = source.fdkIds))
            }
        }

        logger.info(
            "Started ${startedRuns.size} forced harvest(s) for ${fdkIds.size} ${dataType.value} resource(s), ${notFound.size} not found",
        )
        return ForceHarvestResponse(startedRuns = startedRuns, notFound = notFound.sorted())
    }
}
