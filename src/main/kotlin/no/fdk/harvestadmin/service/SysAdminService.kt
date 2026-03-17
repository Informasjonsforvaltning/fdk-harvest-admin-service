package no.fdk.harvestadmin.service

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.kafka.KafkaHarvestEventPublisher
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ResourceToRemove
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class SysAdminService(
    private val kafkaHarvestEventPublisher: KafkaHarvestEventPublisher,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
    private val harvestRunService: HarvestRunService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val adminSourceId = "sys-admin-operations"

    fun publishRemovingEvents(
        dataType: DataType,
        resources: List<ResourceToRemove>,
    ) {
        val timestamp = Instant.now()

        resources.forEach { resource ->
            val runId = UUID.randomUUID().toString()

            // Create harvest run with the generated runId
            val run =
                HarvestRunEntity(
                    runId = runId,
                    dataSourceId = adminSourceId,
                    dataType = dataType.name,
                    runStartedAt = timestamp,
                    status = "IN_PROGRESS",
                    removeAll = false,
                    forced = false,
                )
            val savedRun = harvestRunRepository.save(run)

            // Record metrics for run started
            harvestMetricsService.recordRunStarted(savedRun)

            val event =
                HarvestEvent
                    .newBuilder()
                    .setPhase(HarvestPhase.REMOVING)
                    .setRunId(runId)
                    .setDataType(mapDataType(dataType))
                    .setDataSourceId(adminSourceId)
                    .setDataSourceUrl(null)
                    .setAcceptHeader(null)
                    .setFdkId(resource.fdkId)
                    .setResourceUri(resource.resourceUri)
                    .setStartTime(timestamp.toString())
                    .setEndTime(timestamp.toString())
                    .setErrorMessage(null)
                    .setChangedResourcesCount(null)
                    .setRemovedResourcesCount(null)
                    .setRemoveAll(null)
                    .setForced(false)
                    .build()
            harvestRunService.persistEvent(event)

            // Publish harvest removing event to Kafka
            kafkaHarvestEventPublisher.publishEvent(event)
        }

        logger.info(
            "Published ${resources.size} REMOVING event(s) (dataType=${dataType.value})",
        )
    }

    private fun mapDataType(dataType: DataType): no.fdk.harvest.DataType =
        when (dataType) {
            DataType.CONCEPT -> no.fdk.harvest.DataType.concept
            DataType.DATASET -> no.fdk.harvest.DataType.dataset
            DataType.INFORMATION_MODEL -> no.fdk.harvest.DataType.informationmodel
            DataType.DATA_SERVICE -> no.fdk.harvest.DataType.dataservice
            DataType.PUBLIC_SERVICE -> no.fdk.harvest.DataType.publicService
            DataType.EVENT -> no.fdk.harvest.DataType.event
        }
}
