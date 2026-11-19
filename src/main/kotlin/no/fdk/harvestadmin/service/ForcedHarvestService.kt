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

        if (lookup.notFound.isNotEmpty()) {
            logger.warn("Resources not found in harvester: ${lookup.notFound.sorted()}")
        }

        lookup.sources.forEach { source ->
            val sourceDataType = DATA_TYPE_BY_RESOURCE_TYPE[source.resourceType]
            val dataSource =
                if (sourceDataType == dataType) {
                    dataSourceRepository.findByUrlAndDataType(source.dataSourceUrl, dataType).singleOrNull { it.active }
                } else {
                    null
                }
            if (dataSource == null) {
                logger.warn(
                    "No single active ${dataType.value} data source for ${source.resourceType} at ${source.dataSourceUrl}, skipping ${source.fdkIds}",
                )
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

    companion object {
        private val DATA_TYPE_BY_RESOURCE_TYPE =
            mapOf(
                "CONCEPT" to DataType.CONCEPT,
                "DATASET" to DataType.DATASET,
                "INFORMATIONMODEL" to DataType.INFORMATION_MODEL,
                "DATASERVICE" to DataType.DATA_SERVICE,
                "SERVICE" to DataType.PUBLIC_SERVICE,
                "EVENT" to DataType.EVENT,
            )
    }
}
