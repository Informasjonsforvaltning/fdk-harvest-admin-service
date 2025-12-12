package no.fdk.harvestadmin.config

import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Contact
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.info.License
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import io.swagger.v3.oas.models.servers.Server
import jakarta.servlet.ServletContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig(
    @Value("\${app.openapi.server-url:}") private val serverUrl: String?,
    @Value("\${server.port:8080}") private val serverPort: Int,
) {
    @Bean
    fun customOpenAPI(servletContext: ServletContext?): OpenAPI {
        val baseUrl =
            when {
                !serverUrl.isNullOrBlank() && serverUrl.startsWith("http") -> serverUrl
                !serverUrl.isNullOrBlank() -> "http://localhost:$serverUrl"
                else -> "/" // Use relative URL to use current domain
            }

        return OpenAPI()
            .info(
                Info()
                    .title("National Data Directory FDK Harvest Admin API")
                    .description("Exposes a basic service which keeps track of data sources to be harvested.")
                    .version("1.0")
                    .contact(
                        Contact()
                            .name("Digitaliseringsdirektoratet")
                            .url("https://fellesdatakatalog.digdir.no")
                            .email("fellesdatakatalog@digdir.no"),
                    ).license(
                        License()
                            .name("License of API")
                            .url("http://data.norge.no/nlod/no/2.0"),
                    ),
            ).servers(
                listOf(
                    Server()
                        .url(baseUrl)
                        .description("Current server"),
                ),
            ).components(
                Components()
                    .addSecuritySchemes(
                        "bearer-jwt",
                        SecurityScheme()
                            .type(SecurityScheme.Type.HTTP)
                            .scheme("bearer")
                            .bearerFormat("JWT")
                            .description("OAuth2 JWT token from Keycloak"),
                    ).addSecuritySchemes(
                        "api-key",
                        SecurityScheme()
                            .type(SecurityScheme.Type.APIKEY)
                            .`in`(SecurityScheme.In.HEADER)
                            .name("X-API-Key")
                            .description("API key for internal endpoints"),
                    ),
            ).addSecurityItem(
                SecurityRequirement().addList("api-key"),
            )
    }
}
