package no.fdk.harvestadmin.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.stereotype.Service
import java.util.regex.Pattern

@Service
class SecurityService(
    @param:Value("\${app.security.org-type}") private val orgType: String,
) {
    private fun extractAuthorities(authentication: Authentication?): String? {
        if (authentication == null) {
            return null
        }

        val principal = authentication.principal
        // API key auth uses non-JWT principal; treat as system admin (full access)
        if (principal !is Jwt) {
            return "system:root:admin"
        }

        return principal.claims["authorities"] as? String
    }

    /**
     * Returns organizations the current user may access. Null means all organizations (system admin).
     * API key authentication is treated as system admin (same access as system:root:admin).
     * Returns empty list when not authenticated (no access); only authenticated principals get null for full access.
     */
    fun getAuthorizedOrganizations(authentication: Authentication?): List<String>? {
        val authorities = extractAuthorities(authentication) ?: return emptyList()

        if (hasSystemAdminRole(authorities)) {
            return null // null means all organizations
        }

        return extractOrganizations(authorities)
    }

    fun hasSystemAdminAccess(authentication: Authentication?): Boolean {
        val authorities = extractAuthorities(authentication) ?: return false
        return hasSystemAdminRole(authorities)
    }

    private fun hasSystemAdminRole(authorities: String): Boolean = authorities.contains("system:root:admin")

    fun hasOrganizationRole(
        authorities: String,
        org: String,
        role: String,
    ): Boolean {
        val orgAuth = "$orgType:$org:$role"
        return authorities.contains(orgAuth)
    }

    fun hasAnyOrgAuth(authorities: String): Boolean = authorities.contains(orgType)

    private fun extractOrganizations(authorities: String): List<String> {
        val pattern = Pattern.compile(Pattern.quote(orgType) + ":([^:]+):")
        val matcher = pattern.matcher(authorities)
        val orgs = mutableListOf<String>()

        while (matcher.find()) {
            orgs.add(matcher.group(1))
        }

        return orgs
    }
}
