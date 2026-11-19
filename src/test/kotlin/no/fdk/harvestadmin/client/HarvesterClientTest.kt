package no.fdk.harvestadmin.client

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress

class HarvesterClientTest {
    private val requests = mutableListOf<String>()
    private val apiKeys = mutableListOf<String?>()
    private val server =
        HttpServer.create(InetSocketAddress("localhost", 0), 0).apply {
            createContext("/sources/lookup") { exchange ->
                requests.add(exchange.requestBody.readAllBytes().decodeToString())
                apiKeys.add(exchange.requestHeaders.getFirst("X-API-KEY"))
                val body =
                    """{"sources":[{"dataSourceUrl":"https://a.example/catalog","resourceType":"DATASET","fdkIds":["a1"]}],"notFound":["x"]}"""
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, body.length.toLong())
                exchange.responseBody.use { it.write(body.toByteArray()) }
            }
            start()
        }

    @AfterEach
    fun tearDown() {
        server.stop(0)
    }

    @Test
    fun `looks up sources in chunks and merges the result`() {
        val client = HarvesterClient("http://localhost:${server.address.port}", "test-key")
        val fdkIds = (1..1500).map { "id-$it" }

        val response = client.lookupSources(fdkIds)

        assertEquals(2, requests.size)
        assertEquals(listOf("test-key", "test-key"), apiKeys)
        assertTrue(requests.first().contains("\"id-1\""))
        assertEquals(listOf(HarvestedSource("https://a.example/catalog", "DATASET", listOf("a1"))), response.sources)
        assertEquals(listOf("x", "x"), response.notFound)
    }
}
