package no.fdk.harvestadmin.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "harvest_progress_events")
data class HarvestEventEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    @Column(name = "event_type", nullable = false, length = 100)
    val eventType: String,
    @Column(name = "data_source_id", nullable = false, length = 255)
    val dataSourceId: String,
    @Column(name = "harvest_run_id", length = 36)
    val runId: String? = null,
    @Column(name = "data_type", nullable = false, length = 50)
    val dataType: String,
    @Column(name = "data_source_url", length = 2048)
    val dataSourceUrl: String? = null,
    @Column(name = "accept_header", length = 255)
    val acceptHeader: String? = null,
    @Column(name = "fdk_id", length = 255)
    val fdkId: String? = null,
    @Column(name = "resource_uri", length = 2048)
    val resourceUri: String? = null,
    @Column(name = "start_time", length = 255)
    val startTime: String? = null,
    @Column(name = "end_time", length = 255)
    val endTime: String? = null,
    @Column(name = "error_message", columnDefinition = "TEXT")
    val errorMessage: String? = null,
    @Column(name = "changed_resources_count")
    val changedResourcesCount: Int? = null,
    @Column(name = "unchanged_resources_count")
    val unchangedResourcesCount: Int? = null,
    @Column(name = "removed_resources_count")
    val removedResourcesCount: Int? = null,
    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: Instant = Instant.now(),
)
