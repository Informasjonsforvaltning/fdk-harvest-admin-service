package no.fdk.harvestadmin.integration

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Verifies response compression through the real embedded server.
 * Uses the OpenAPI document (springdoc.api-docs.path = /api-docs), which is permitted without
 * authentication and is well above the 1 KB threshold.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CompressionIntegrationTest : BaseIntegrationTest() {
    @LocalServerPort
    private var port = 0

    // java.net.http never decompresses on its own, so Content-Encoding and the body are seen as sent
    private val client = HttpClient.newHttpClient()

    private fun get(acceptEncoding: String?): HttpResponse<ByteArray> {
        val builder = HttpRequest.newBuilder(URI("http://localhost:$port/api-docs")).GET()
        if (acceptEncoding != null) builder.header("Accept-Encoding", acceptEncoding)
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
    }

    @Test
    fun `gzips the response when the client accepts it`() {
        val response = get("gzip")
        assertEquals(200, response.statusCode())
        assertEquals("gzip", response.headers().firstValue("Content-Encoding").orElse(null))
        assertTrue(response.headers().allValues("Vary").joinToString(",").lowercase().contains("accept-encoding"))
        val body = response.body()
        assertTrue(body.size > 2 && body[0] == 0x1f.toByte() && body[1] == 0x8b.toByte(), "body is not gzip")
    }

    @Test
    fun `leaves the response uncompressed when the client does not ask`() {
        val response = get(null)
        assertEquals(200, response.statusCode())
        assertTrue(response.headers().firstValue("Content-Encoding").isEmpty)
        assertTrue(response.body().size > 1024, "test endpoint must exceed the 1 KB threshold")
    }
}
