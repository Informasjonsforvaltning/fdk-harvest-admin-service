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
    /**
     * Returns organizations the current user may access. Null means all organizations (system admin).
     * API key authentication is treated as system admin (same access as system:root:admin).
     * Returns empty list when not authenticated (no access); only authenticated principals get null for full access.
     */
    fun getAuthorizedOrganizations(authentication: Authentication?): List<String>? {
        if (authentication == null) {
            return emptyList()
        }

        val principal = authentication.principal
        // API key auth uses non-JWT principal; treat as system admin (full access)
        if (principal !is Jwt) {
            return null
        }

        val authorities = principal.claims["authorities"] as? String ?: return null

        if (hasSystemAdminRole(authorities)) {
            return null // null means all organizations
        }

        return extractOrganizations(authorities)
    }

    fun hasSystemAdminRole(authorities: String): Boolean = authorities.contains("system:root:admin")

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
