package no.fdk.harvestadmin.client

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import java.time.Duration

data class CurationFile(val content: String, val version: String?)

@Component
class CurationClient(@param:Value("\${app.curation.uri}") private val curationUri: String) {
    private val restClient =
        RestClient
            .builder()
            .requestFactory(
                SimpleClientHttpRequestFactory().apply {
                    setConnectTimeout(Duration.ofSeconds(10))
                    setReadTimeout(Duration.ofSeconds(30))
                },
            ).build()

    fun fetch(): CurationFile {
        val response =
            restClient
                .get()
                .uri(curationUri)
                .retrieve()
                .toEntity(String::class.java)
        return CurationFile(
            content = response.body ?: "",
            version = response.headers.getFirst("X-Curation-Version"),
        )
    }
}
