package no.fdk.harvestadmin.config

import no.fdk.harvestadmin.service.SecurityService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

@Configuration
@EnableWebSecurity
class SecurityConfig(
    @Value("\${app.security.token-audience}") private val tokenAudience: String,
    @Value("\${app.security.api-key}") private val apiKey: String,
    @Value("\${app.security.keycloak-host}") private val keycloakHost: String,
    @Value("\${app.cors.origin-patterns}") private val corsOriginPatterns: String,
    private val securityService: SecurityService,
) {
    @Bean
    fun filterChain(http: HttpSecurity): SecurityFilterChain {
        // Add API key filter for all endpoints (temporary replacement for JWT)
        if (apiKey.isNotBlank()) {
            http.addFilterBefore(
                ApiKeyAuthenticationFilter(apiKey),
                UsernamePasswordAuthenticationFilter::class.java,
            )
        }

        return http
            .csrf { it.disable() }
            .cors { it.configurationSource(corsConfigurationSource()) }
            .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
            // Temporarily disabled OAuth2 JWT authentication
            // .oauth2ResourceServer { oauth2 ->
            //     oauth2.jwt { jwt ->
            //         jwt
            //             .decoder(jwtDecoder())
            //             .jwtAuthenticationConverter { jwt ->
            //                 // Validate audience
            //                 val claims = jwt.claims
            //                 val audience = claims["aud"] as? List<*> ?: emptyList<Any>()
            //                 if (!audience.contains(tokenAudience)) {
            //                     throw IllegalArgumentException("Invalid audience")
            //                 }
            //                 // Convert Jwt to JwtAuthenticationToken
            //                 org.springframework.security.oauth2.server.resource.authentication
            //                     .JwtAuthenticationToken(jwt)
            //             }
            //     }
            // }
            .authorizeHttpRequests { authz ->
                authz
                    // Actuator endpoints - public
                    .requestMatchers("/actuator/**")
                    .permitAll()
                    // Swagger UI and API docs - public
                    .requestMatchers(
                        "/swagger-ui/**",
                        "/swagger-ui.html",
                        "/swagger-resources/**",
                        "/v3/api-docs/**",
                        "/api-docs/**",
                        "/webjars/**",
                    ).permitAll()
                    // Temporary: Create data source endpoint - public (POST only)
                    .requestMatchers(HttpMethod.POST, "/organizations/*/datasources")
                    .permitAll()
                    // All endpoints - require API key authentication (temporary replacement for JWT)
                    .anyRequest()
                    .hasRole("API_USER")
            }.build()
    }

    // Temporarily disabled - using API key authentication instead of JWT
    // @Bean
    // fun jwtDecoder(): JwtDecoder {
    //     val jwkSetUri = "$keycloakHost/realms/fdk/protocol/openid-connect/certs"
    //     return NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build()
    // }

    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource {
        val configuration = CorsConfiguration()
        configuration.allowedOriginPatterns = corsOriginPatterns.split(",").map { it.trim() }
        configuration.allowedMethods = listOf("GET", "POST", "PUT", "DELETE", "OPTIONS")
        configuration.allowedHeaders = listOf("*")
        configuration.allowCredentials = true
        configuration.maxAge = 3600L

        val source = UrlBasedCorsConfigurationSource()
        source.registerCorsConfiguration("/**", configuration)
        return source
    }
}
