package no.fdk.harvestadmin.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "harvest_runs")
data class HarvestRunEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    @Column(name = "run_id", nullable = false, unique = true, length = 36)
    val runId: String, // UUID for correlation with events
    @Column(name = "data_source_id", nullable = false, length = 255)
    val dataSourceId: String,
    @Column(name = "data_type", nullable = false, length = 50)
    val dataType: String,
    @Column(name = "run_started_at", nullable = false)
    val runStartedAt: Instant,
    @Column(name = "run_ended_at")
    val runEndedAt: Instant? = null,
    // Current state fields (for tracking current phase during run)
    @Column(name = "current_phase", length = 100)
    val currentPhase: String? = null,
    @Column(name = "phase_started_at")
    val phaseStartedAt: Instant? = null,
    @Column(name = "last_event_timestamp")
    val lastEventTimestamp: Long? = null,
    @Column(name = "processed_resources")
    val processedResources: Int? = null,
    @Column(name = "remaining_resources")
    val remainingResources: Int? = null,
    @Column(name = "partially_processed_resources")
    val partiallyProcessedResources: Int? = null,
    // Phase timings (in milliseconds)
    @Column(name = "init_duration_ms")
    val initDurationMs: Long? = null,
    @Column(name = "harvest_duration_ms")
    val harvestDurationMs: Long? = null,
    @Column(name = "reasoning_duration_ms")
    val reasoningDurationMs: Long? = null,
    @Column(name = "rdf_parsing_duration_ms")
    val rdfParsingDurationMs: Long? = null,
    @Column(name = "search_processing_duration_ms")
    val searchProcessingDurationMs: Long? = null,
    @Column(name = "ai_search_processing_duration_ms")
    val aiSearchProcessingDurationMs: Long? = null,
    @Column(name = "api_processing_duration_ms")
    val apiProcessingDurationMs: Long? = null,
    @Column(name = "sparql_processing_duration_ms")
    val sparqlProcessingDurationMs: Long? = null,
    @Column(name = "total_duration_ms")
    val totalDurationMs: Long? = null,
    // Resource counts
    @Column(name = "total_resources")
    val totalResources: Int? = null,
    @Column(name = "changed_resources_count")
    val changedResourcesCount: Int? = null,
    @Column(name = "unchanged_resources_count")
    val unchangedResourcesCount: Int? = null,
    @Column(name = "removed_resources_count")
    val removedResourcesCount: Int? = null,
    // Status
    @Column(name = "status", nullable = false, length = 50)
    val status: String, // COMPLETED, FAILED, IN_PROGRESS
    @Column(name = "error_message", columnDefinition = "TEXT")
    val errorMessage: String? = null,
    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: Instant = Instant.now(),
    @Column(name = "updated_at", nullable = false, updatable = true)
    val updatedAt: Instant = Instant.now(),
)
