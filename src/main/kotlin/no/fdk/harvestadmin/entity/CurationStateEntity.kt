package no.fdk.harvestadmin.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "curation_state")
class CurationStateEntity(
    @Id
    @Column(name = "id")
    var id: String,
    @Column(name = "processed_content", nullable = false, columnDefinition = "TEXT")
    var processedContent: String,
    @Column(name = "processed_version")
    var processedVersion: String? = null,
    @Column(name = "pending_content", columnDefinition = "TEXT")
    var pendingContent: String? = null,
    @Column(name = "pending_version")
    var pendingVersion: String? = null,
    @Column(name = "pending_since")
    var pendingSince: Instant? = null,
)
