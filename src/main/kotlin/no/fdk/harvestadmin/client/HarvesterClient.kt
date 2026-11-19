package no.fdk.harvestadmin.client

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.body
import java.time.Duration

data class SourceLookupRequest(val fdkIds: List<String>)

data class SourceLookupResponse(val sources: List<HarvestedSource> = emptyList(), val notFound: List<String> = emptyList())

data class HarvestedSource(val dataSourceUrl: String, val resourceType: String, val fdkIds: List<String>)

private const val MAX_FDK_IDS_PER_LOOKUP = 1000

@Component
class HarvesterClient(@Value("\${app.harvester.uri}") harvesterUri: String, @Value("\${app.harvester.api-key}") apiKey: String) {
    private val restClient =
        RestClient
            .builder()
            .baseUrl(harvesterUri)
            .defaultHeader("X-API-KEY", apiKey)
            .requestFactory(
                SimpleClientHttpRequestFactory().apply {
                    setConnectTimeout(Duration.ofSeconds(10))
                    setReadTimeout(Duration.ofSeconds(60))
                },
            ).build()

    fun lookupSources(fdkIds: Collection<String>): SourceLookupResponse {
        val responses =
            fdkIds.distinct().chunked(MAX_FDK_IDS_PER_LOOKUP).map { chunk ->
                restClient
                    .post()
                    .uri("/sources/lookup")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(SourceLookupRequest(chunk))
                    .retrieve()
                    .body<SourceLookupResponse>()
                    ?: throw IllegalStateException("Empty response from harvester source lookup")
            }
        val sources =
            responses
                .flatMap { it.sources }
                .groupBy { it.dataSourceUrl to it.resourceType }
                .map { (key, sources) -> HarvestedSource(key.first, key.second, sources.flatMap { it.fdkIds }.distinct()) }
        return SourceLookupResponse(sources = sources, notFound = responses.flatMap { it.notFound })
    }
}
