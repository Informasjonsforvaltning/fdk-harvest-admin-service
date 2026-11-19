package no.fdk.harvestadmin.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CuratedDatasetTypesTest {
    private val header = "fdkId,title,eu-hvd,national-hvd,national-basic-data"

    @Test
    fun `finds added, removed and changed datasets`() {
        val previous =
            """
            $header
            a,A,ja,nei,nei
            b,B,nei,ja,nei
            c,C,nei,nei,ja
            """.trimIndent()
        val current =
            """
            $header
            a,A,ja,nei,nei
            b,B,nei,ja,ja
            d,D,ja,nei,nei
            """.trimIndent()

        assertEquals(setOf("b", "c", "d"), changedFdkIds(previous, current))
    }

    @Test
    fun `ignores title and case changes`() {
        val previous = "$header\na,A,ja,nei,nei"
        val current = "$header\na,\"Ny tittel, med komma\",JA,Nei,nei"

        assertEquals(emptySet<String>(), changedFdkIds(previous, current))
    }

    @Test
    fun `treats new type column as change for datasets marked with it`() {
        val previous = "fdkId,title,eu-hvd\na,A,ja\nb,B,nei"
        val current = "fdkId,title,eu-hvd,national-hvd\na,A,ja,nei\nb,B,nei,ja"

        assertEquals(setOf("a", "b"), changedFdkIds(previous, current))
    }
}
