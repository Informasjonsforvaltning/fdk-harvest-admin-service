package no.fdk.harvestadmin.config

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource
import org.springframework.web.filter.OncePerRequestFilter

class ApiKeyAuthenticationFilter(
    private val apiKey: String,
) : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        // Process API key authentication for all endpoints (temporary replacement for JWT)
        val path = request.requestURI
        // Skip API key check for public endpoints
        if (!path.startsWith("/actuator") &&
            !path.startsWith("/swagger-ui") &&
            !path.startsWith("/swagger-resources") &&
            !path.startsWith("/v3/api-docs") &&
            !path.startsWith("/api-docs") &&
            !path.startsWith("/webjars")
        ) {
            val requestApiKey = request.getHeader("X-API-KEY")

            if (requestApiKey != null && requestApiKey == apiKey) {
                val authorities = listOf(SimpleGrantedAuthority("ROLE_API_USER"))
                val authentication =
                    UsernamePasswordAuthenticationToken(
                        "api-user",
                        null,
                        authorities,
                    )
                authentication.details = WebAuthenticationDetailsSource().buildDetails(request)
                SecurityContextHolder.getContext().authentication = authentication
            }
        }

        filterChain.doFilter(request, response)
    }
}
