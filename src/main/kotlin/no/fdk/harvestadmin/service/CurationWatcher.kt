package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.client.CurationClient
import no.fdk.harvestadmin.entity.CurationStateEntity
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.repository.CurationStateRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration

const val CURATED_DATASET_TYPES_ID = "high-value-dataset-types"

@Service
class CurationWatcher(
    private val curationClient: CurationClient,
    private val curationStateRepository: CurationStateRepository,
    private val forcedHarvestService: ForcedHarvestService,
    @param:Value("\${app.curation.force-delay}") private val forceDelay: Duration,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${app.curation.poll-interval}", initialDelayString = "\${app.curation.poll-interval}")
    fun poll() {
        try {
            checkForChanges()
        } catch (e: Exception) {
            logger.error("Checking curated dataset types for changes failed", e)
        }
    }

    fun checkForChanges() {
        val file = curationClient.fetch()
        parseCuratedDatasetTypes(file.content)
        val now = clock.instant()
        val state = curationStateRepository.findById(CURATED_DATASET_TYPES_ID).orElse(null)

        if (state == null) {
            curationStateRepository.save(
                CurationStateEntity(id = CURATED_DATASET_TYPES_ID, processedContent = file.content, processedVersion = file.version),
            )
            logger.info("Stored initial curated dataset types version ${file.version}")
            return
        }

        if (file.content != (state.pendingContent ?: state.processedContent)) {
            if (changedFdkIds(state.processedContent, file.content).isEmpty()) {
                state.processedContent = file.content
                state.processedVersion = file.version
                state.pendingContent = null
                state.pendingVersion = null
                state.pendingSince = null
            } else {
                state.pendingContent = file.content
                state.pendingVersion = file.version
                state.pendingSince = now
                logger.info("Curated dataset types version ${file.version} detected, forcing harvest after $forceDelay")
            }
            curationStateRepository.save(state)
        }

        val pendingContent = state.pendingContent ?: return
        val pendingSince = state.pendingSince ?: return
        if (now.isBefore(pendingSince.plus(forceDelay))) return

        val changed = changedFdkIds(state.processedContent, pendingContent)
        forcedHarvestService.forceHarvest(DataType.DATASET, changed)
        logger.info("Forced harvest of ${changed.size} dataset(s) for curated dataset types version ${state.pendingVersion}")

        state.processedContent = pendingContent
        state.processedVersion = state.pendingVersion
        state.pendingContent = null
        state.pendingVersion = null
        state.pendingSince = null
        curationStateRepository.save(state)
    }
}
