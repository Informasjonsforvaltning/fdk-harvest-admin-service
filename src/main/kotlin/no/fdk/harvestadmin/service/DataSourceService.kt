package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.entity.DataSourceEntity
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.exception.ConflictException
import no.fdk.harvestadmin.exception.NotFoundException
import no.fdk.harvestadmin.exception.ValidationException
import no.fdk.harvestadmin.kafka.KafkaHarvestEventPublisher
import no.fdk.harvestadmin.model.DataSource
import no.fdk.harvestadmin.model.DataSourceType
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.rabbit.RabbitMQPublisher
import no.fdk.harvestadmin.repository.DataSourceRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import no.fdk.harvestadmin.service.HarvestRunService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

@Service
class DataSourceService(
    private val dataSourceRepository: DataSourceRepository,
    private val rabbitMQPublisher: RabbitMQPublisher,
    private val harvestRunService: HarvestRunService,
    private val kafkaHarvestEventPublisher: KafkaHarvestEventPublisher,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun getAllowedDataSources(
        authorizedOrgs: List<String>?,
        dataType: DataType?,
        dataSourceType: DataSourceType?,
    ): List<DataSource> {
        try {
            val entities = dataSourceRepository.findByFilters(authorizedOrgs, dataType, dataSourceType)
            return entities.map { it.toModel() }
        } catch (e: Exception) {
            logger.error("Error getting data sources", e)
            throw RuntimeException("Error getting data sources", e)
        }
    }

    fun getDataSource(id: String): DataSource {
        try {
            val entity =
                dataSourceRepository
                    .findById(id)
                    .orElseThrow { NotFoundException("Data source not found with id: $id") }
            return entity.toModel()
        } catch (e: NotFoundException) {
            throw e
        } catch (e: Exception) {
            logger.error("Error getting data source with id: $id", e)
            throw RuntimeException("Error getting data source", e)
        }
    }

    @Transactional
    fun createDataSource(
        dataSource: DataSource,
        org: String,
    ): DataSource {
        if (org != dataSource.publisherId) {
            throw ValidationException("Trying to create data source for other organization")
        }

        val existing = dataSourceRepository.findByUrlAndDataType(dataSource.url, dataSource.dataType)
        if (existing.isNotEmpty()) {
            throw ConflictException("Trying to recreate existing data source")
        }

        val id = UUID.randomUUID().toString()
        val entity = DataSourceEntity.fromModel(dataSource.copy(id = id))

        return try {
            val saved = dataSourceRepository.save(entity)
            saved.toModel()
        } catch (e: Exception) {
            logger.error("Error creating data source", e)
            throw RuntimeException("Error creating data source", e)
        }
    }

    @Transactional
    fun createDataSourceFromRabbitMessage(dataSource: DataSource) {
        // Note: Bean Validation is not automatically triggered for RabbitMQ messages
        // Basic validation is done manually here
        if (dataSource.url.isBlank() || dataSource.publisherId.isBlank()) {
            logger.error("Invalid data source from RabbitMQ: missing required fields")
            return
        }

        val existing = dataSourceRepository.findByUrlAndDataType(dataSource.url, dataSource.dataType)
        if (existing.isNotEmpty()) {
            logger.warn("Data source with current url and data type already exists, skipping creation from RabbitMQ")
            return
        }

        val id = UUID.randomUUID().toString()
        val entity = DataSourceEntity.fromModel(dataSource.copy(id = id))

        try {
            dataSourceRepository.save(entity)
            logger.info("Data source created from RabbitMQ message with id: $id")
        } catch (e: Exception) {
            logger.error("Error creating data source from RabbitMQ", e)
        }
    }

    @Transactional
    fun updateDataSource(
        id: String,
        dataSource: DataSource,
        org: String,
    ): DataSource {
        val existing =
            dataSourceRepository
                .findById(id)
                .orElseThrow { NotFoundException("Data source not found with id: $id") }

        if (org != existing.publisherId) {
            throw ValidationException("Trying to update data source for other organization")
        }

        val conflicting = dataSourceRepository.findByUrlAndDataType(dataSource.url, dataSource.dataType)
        if (conflicting.isNotEmpty() && conflicting.any { it.id != id }) {
            throw ConflictException("Source not unique")
        }

        existing.updateFromModel(dataSource)

        return try {
            val saved = dataSourceRepository.save(existing)
            saved.toModel()
        } catch (e: Exception) {
            logger.error("Error updating data source", e)
            throw RuntimeException("Error updating data source", e)
        }
    }

    @Transactional
    fun deleteDataSource(id: String) {
        try {
            if (!dataSourceRepository.existsById(id)) {
                throw NotFoundException("Data source not found with id: $id")
            }
            dataSourceRepository.deleteById(id)
        } catch (e: NotFoundException) {
            throw e
        } catch (e: Exception) {
            logger.error("Error deleting data source with id: $id", e)
            throw RuntimeException("Error deleting data source", e)
        }
    }

    fun startHarvesting(
        id: String,
        org: String,
    ) {
        val dataSource =
            dataSourceRepository
                .findById(id)
                .orElseThrow { NotFoundException("Data source not found with id: $id") }

        if (org != dataSource.publisherId) {
            throw ValidationException("Trying to start harvest for other organization")
        }

        try {
            val timestamp = Instant.now()

            // Generate a unique run ID for this harvest
            val runId = UUID.randomUUID().toString()

            // Create harvest run with the generated runId
            val run =
                HarvestRunEntity(
                    runId = runId,
                    dataSourceId = id,
                    dataType = dataSource.dataType.name,
                    runStartedAt = timestamp,
                    status = "IN_PROGRESS",
                )
            val savedRun = harvestRunRepository.save(run)

            // Record metrics for run started
            harvestMetricsService.recordRunStarted(savedRun)

            // Create harvest trigger event with the run ID
            val triggerEvent =
                no.fdk.harvest.HarvestEvent
                    .newBuilder()
                    .setPhase(no.fdk.harvest.HarvestPhase.INITIATING)
                    .setDataSourceId(id)
                    .setRunId(runId)
                    .setDataType(mapDataType(dataSource.dataType))
                    .setDataSourceUrl(dataSource.url)
                    .setAcceptHeader(dataSource.acceptHeader)
                    .setFdkId(null)
                    .setResourceUri(null)
                    .setStartTime(timestamp.toString())
                    .setEndTime(timestamp.toString())
                    .setErrorMessage(null)
                    .setChangedResourcesCount(null)
                    .setUnchangedResourcesCount(null)
                    .setRemovedResourcesCount(null)
                    .build()
            harvestRunService.persistEvent(triggerEvent)

            // Publish harvest trigger event to Kafka
            kafkaHarvestEventPublisher.publishEvent(triggerEvent)

            // Publish harvest trigger to RabbitMQ (deprecated)
            val trigger =
                no.fdk.harvestadmin.model.HarvestTrigger(
                    runId = runId,
                    dataSourceId = id,
                    dataSourceUrl = dataSource.url,
                    dataType = dataSource.dataType.value,
                    acceptHeader = dataSource.acceptHeader!!,
                    publisherId = dataSource.publisherId,
                    timestamp = 0L,
                    forceUpdate = "true",
                )
            rabbitMQPublisher.publishHarvestTrigger(dataSource.dataType, trigger)
        } catch (e: Exception) {
            logger.error("Error starting harvest for data source with id: $id", e)
            throw RuntimeException("Error starting harvest", e)
        }
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
