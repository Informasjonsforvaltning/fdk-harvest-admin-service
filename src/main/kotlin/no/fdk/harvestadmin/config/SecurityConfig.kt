package no.fdk.harvestadmin.config

import no.fdk.harvestadmin.service.SecurityService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.core.authority.SimpleGrantedAuthority
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
    @param:Value("\${app.security.token-audience}") private val tokenAudience: String,
    @param:Value("\${app.security.api-key}") private val apiKey: String,
    @param:Value("\${app.security.keycloak-host}") private val keycloakHost: String,
    @param:Value("\${app.cors.origin-patterns}") private val corsOriginPatterns: String,
    private val securityService: SecurityService,
) {
    @Bean
    fun filterChain(http: HttpSecurity): SecurityFilterChain {
        // API key filter first: if X-API-KEY is valid, sets authentication (JWT used when no API key)
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
            .oauth2ResourceServer { oauth2 ->
                oauth2.jwt { jwt ->
                    jwt
                        .decoder(jwtDecoder())
                        .jwtAuthenticationConverter { jwt ->
                            val audience =
                                when (val aud = jwt.claims["aud"]) {
                                    is String -> listOf(aud)
                                    is List<*> -> aud.mapNotNull { it?.toString() }
                                    else -> emptyList()
                                }
                            if (!audience.contains(tokenAudience)) {
                                throw IllegalArgumentException("Invalid audience")
                            }
                            val authoritiesStr = jwt.claims["authorities"] as? String ?: ""
                            val authorities =
                                authoritiesStr
                                    .split(",")
                                    .map { it.trim() }
                                    .filter { it.isNotBlank() }
                                    .map { SimpleGrantedAuthority(it) }
                            org.springframework.security.oauth2.server.resource.authentication
                                .JwtAuthenticationToken(jwt, authorities)
                        }
                }
            }.authorizeHttpRequests { authz ->
                authz
                    .requestMatchers(HttpMethod.OPTIONS, "/**")
                    .permitAll()
                    .requestMatchers("/actuator/**")
                    .permitAll()
                    .requestMatchers(
                        "/swagger-ui/**",
                        "/swagger-ui.html",
                        "/swagger-resources/**",
                        "/v3/api-docs/**",
                        "/api-docs/**",
                        "/webjars/**",
                    ).permitAll()
                    .anyRequest()
                    .authenticated()
            }.build()
    }

    @Bean
    fun jwtDecoder(): JwtDecoder {
        val jwkSetUri = "$keycloakHost/realms/fdk/protocol/openid-connect/certs"
        return NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build()
    }

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
