package no.fdk.harvestadmin.service

import no.fdk.harvestadmin.client.CurationClient
import no.fdk.harvestadmin.client.CurationFile
import no.fdk.harvestadmin.entity.CurationStateEntity
import no.fdk.harvestadmin.model.DataType
import no.fdk.harvestadmin.model.ForceHarvestResponse
import no.fdk.harvestadmin.repository.CurationStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Optional

private const val HEADER = "fdkId,title,eu-hvd,national-hvd,national-basic-data"
private const val V1 = "$HEADER\na,A,ja,nei,nei\nb,B,nei,ja,nei"
private const val V2 = "$HEADER\na,A,ja,nei,nei\nb,B,nei,nei,nei\nc,C,nei,nei,ja"

private class MutableClock(var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId?): Clock = this

    override fun instant(): Instant = now
}

class CurationWatcherTest {
    private val curationClient: CurationClient = mock()
    private val forcedHarvestService: ForcedHarvestService = mock()
    private val clock = MutableClock(Instant.parse("2026-11-18T10:00:00Z"))
    private var stored: CurationStateEntity? = null
    private val curationStateRepository: CurationStateRepository = mock()
    private val watcher =
        CurationWatcher(curationClient, curationStateRepository, forcedHarvestService, Duration.ofMinutes(6), clock)

    init {
        whenever(curationStateRepository.findById(CURATED_DATASET_TYPES_ID)).thenAnswer { Optional.ofNullable(stored) }
        whenever(curationStateRepository.save(any<CurationStateEntity>())).thenAnswer {
            (it.arguments[0] as CurationStateEntity).also { entity -> stored = entity }
        }
        whenever(forcedHarvestService.forceHarvest(any(), any())).thenReturn(ForceHarvestResponse(emptyList(), emptyList()))
    }

    @Test
    fun `stores first version without forcing harvest`() {
        whenever(curationClient.fetch()).thenReturn(CurationFile(V1, "v1"))

        watcher.checkForChanges()

        assertEquals(V1, stored?.processedContent)
        assertEquals("v1", stored?.processedVersion)
        verify(forcedHarvestService, never()).forceHarvest(any(), any())
    }

    @Test
    fun `forces harvest of changed datasets after delay`() {
        stored = CurationStateEntity(id = CURATED_DATASET_TYPES_ID, processedContent = V1, processedVersion = "v1")
        whenever(curationClient.fetch()).thenReturn(CurationFile(V2, "v2"))

        watcher.checkForChanges()
        assertEquals(V2, stored?.pendingContent)
        verify(forcedHarvestService, never()).forceHarvest(any(), any())

        clock.now = clock.now.plus(Duration.ofMinutes(5))
        watcher.checkForChanges()
        verify(forcedHarvestService, never()).forceHarvest(any(), any())

        clock.now = clock.now.plus(Duration.ofMinutes(1))
        watcher.checkForChanges()
        verify(forcedHarvestService).forceHarvest(eq(DataType.DATASET), eq(setOf("b", "c")))
        assertEquals(V2, stored?.processedContent)
        assertEquals("v2", stored?.processedVersion)
        assertNull(stored?.pendingContent)
    }

    @Test
    fun `drops pending change when file is reverted`() {
        stored =
            CurationStateEntity(
                id = CURATED_DATASET_TYPES_ID,
                processedContent = V1,
                processedVersion = "v1",
                pendingContent = V2,
                pendingVersion = "v2",
                pendingSince = clock.now,
            )
        whenever(curationClient.fetch()).thenReturn(CurationFile(V1, "v3"))

        clock.now = clock.now.plus(Duration.ofMinutes(10))
        watcher.checkForChanges()

        verify(forcedHarvestService, never()).forceHarvest(any(), any())
        assertNull(stored?.pendingContent)
        assertEquals("v3", stored?.processedVersion)
    }

    @Test
    fun `keeps pending change when forced harvest fails`() {
        stored =
            CurationStateEntity(
                id = CURATED_DATASET_TYPES_ID,
                processedContent = V1,
                pendingContent = V2,
                pendingVersion = "v2",
                pendingSince = clock.now.minus(Duration.ofMinutes(10)),
            )
        whenever(curationClient.fetch()).thenReturn(CurationFile(V2, "v2"))
        whenever(forcedHarvestService.forceHarvest(any(), any())).thenThrow(IllegalStateException("harvester down"))

        watcher.poll()

        assertEquals(V2, stored?.pendingContent)
        assertEquals(V1, stored?.processedContent)
    }
}
