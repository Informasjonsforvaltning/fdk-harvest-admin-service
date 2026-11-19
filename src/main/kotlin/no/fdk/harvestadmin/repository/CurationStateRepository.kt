package no.fdk.harvestadmin.repository

import no.fdk.harvestadmin.entity.CurationStateEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface CurationStateRepository : JpaRepository<CurationStateEntity, String>
