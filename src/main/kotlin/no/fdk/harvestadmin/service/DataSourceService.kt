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
import no.fdk.harvestadmin.repository.DataSourceRepository
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.ZonedDateTime
import java.util.UUID

@Service
class DataSourceService(
    private val dataSourceRepository: DataSourceRepository,
    private val harvestEventIngestionService: HarvestEventIngestionService,
    private val kafkaHarvestEventPublisher: KafkaHarvestEventPublisher,
    private val harvestRunRepository: HarvestRunRepository,
    private val harvestMetricsService: HarvestMetricsService,
    @param:Value("\${app.harvest.scheduled-slots:12}") private val scheduledSlots: Int,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun getAllowedDataSources(
        authorizedOrgs: List<String>?,
        dataType: DataType?,
        dataSourceType: DataSourceType?,
    ): List<DataSource> = dataSourceRepository.findByFilters(authorizedOrgs, dataType, dataSourceType).map { it.toModel() }

    fun getDataSource(id: String): DataSource = findDataSource(id).toModel()

    @Transactional
    fun createDataSource(
        dataSource: DataSource,
        org: String,
    ): DataSource {
        requireOwnedBy(dataSource.publisherId, org, "Trying to create data source for other organization")

        val existing = dataSourceRepository.findByUrlAndDataType(dataSource.url, dataSource.dataType)
        if (existing.isNotEmpty()) {
            throw ConflictException("Trying to recreate existing data source")
        }

        val id = UUID.randomUUID().toString()
        val entity = DataSourceEntity.fromModel(dataSource.copy(id = id))
        return dataSourceRepository.save(entity).toModel()
    }

    @Transactional
    fun updateDataSource(
        id: String,
        dataSource: DataSource,
        org: String,
    ): DataSource {
        val existing = findDataSource(id)
        requireOwnedBy(existing, org, "Trying to update data source for other organization")

        val conflicting = dataSourceRepository.findByUrlAndDataType(dataSource.url, dataSource.dataType)
        if (conflicting.isNotEmpty() && conflicting.any { it.id != id }) {
            throw ConflictException("Source not unique")
        }

        existing.updateFromModel(dataSource)
        return dataSourceRepository.save(existing).toModel()
    }

    @Transactional
    fun setDataSourceActive(
        id: String,
        org: String,
        active: Boolean,
    ): DataSource {
        val existing = findDataSource(id)
        requireOwnedBy(existing, org, "Trying to modify data source for other organization")

        existing.active = active
        return dataSourceRepository.save(existing).toModel()
    }

    @Transactional
    fun deleteDataSource(id: String) {
        if (!dataSourceRepository.existsById(id)) {
            throw NotFoundException("Data source not found with id: $id")
        }
        dataSourceRepository.deleteById(id)
    }

    fun startHarvesting(
        id: String,
        org: String,
        removeAll: Boolean? = null,
        forced: Boolean? = null,
    ) {
        val dataSource = findDataSource(id)
        requireOwnedBy(dataSource, org, "Trying to start harvest for other organization")

        if (!dataSource.active) {
            throw ValidationException("Cannot start harvest for inactive data source: $id")
        }

        val timestamp = Instant.now()
        val runId = UUID.randomUUID().toString()

        val run =
            HarvestRunEntity(
                runId = runId,
                dataSourceId = id,
                dataType = dataSource.dataType.name,
                runStartedAt = timestamp,
                status = "IN_PROGRESS",
                removeAll = removeAll,
                forced = forced,
            )
        val savedRun = harvestRunRepository.save(run)
        harvestMetricsService.recordRunStarted(savedRun)

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
                .setRemovedResourcesCount(null)
                .setRemoveAll(removeAll)
                .setForced(forced ?: false)
                .build()
        harvestEventIngestionService.persistEvent(triggerEvent)
        kafkaHarvestEventPublisher.publishEvent(triggerEvent)
    }

    fun startHarvestingByUrlAndDataType(
        org: String,
        url: String,
        dataType: DataType,
    ) {
        val dataSources = dataSourceRepository.findByUrlAndDataType(url, dataType)

        if (dataSources.isEmpty()) {
            throw NotFoundException("Data source not found for url '$url' and data type '${dataType.value}'")
        }

        val matchingOrgSources = dataSources.filter { it.publisherId == org }
        if (matchingOrgSources.isEmpty()) {
            throw ValidationException("Trying to start harvest for other organization")
        }

        if (matchingOrgSources.size > 1) {
            throw ConflictException("Multiple data sources found for url '$url' and data type '${dataType.value}'")
        }

        val dataSource = matchingOrgSources.first()
        startHarvesting(
            id = dataSource.id,
            org = org,
            removeAll = false,
            forced = false,
        )
    }

    @Scheduled(cron = "\${app.harvest.scheduled-cron:0 */5 * * * *}")
    fun scheduledHarvest() {
        startHarvestingAll(forced = false)
    }

    fun startHarvestingAll(forced: Boolean = false) {
        val dataSources = dataSourceRepository.findByFilters(null, null, null)
        val minuteOfHour = ZonedDateTime.now().minute
        val intervalMinutes = 60 / scheduledSlots
        val currentSlot = (minuteOfHour / intervalMinutes) % scheduledSlots
        dataSources
            .filter { ds -> ds.active && (ds.id.hashCode().and(Int.MAX_VALUE) % scheduledSlots) == currentSlot }
            .forEach { ds ->
                try {
                    startHarvesting(ds.id, ds.publisherId, removeAll = null, forced = forced)
                    logger.debug("Scheduled harvest started for data source ${ds.id} (slot $currentSlot)")
                } catch (e: Exception) {
                    logger.error("Failed to start scheduled harvest for data source ${ds.id}", e)
                }
            }
    }

    private fun findDataSource(id: String): DataSourceEntity =
        dataSourceRepository
            .findById(id)
            .orElseThrow { NotFoundException("Data source not found with id: $id") }

    private fun requireOwnedBy(
        entity: DataSourceEntity,
        org: String,
        message: String,
    ) {
        requireOwnedBy(entity.publisherId, org, message)
    }

    private fun requireOwnedBy(
        publisherId: String,
        org: String,
        message: String,
    ) {
        if (org != publisherId) {
            throw ValidationException(message)
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
