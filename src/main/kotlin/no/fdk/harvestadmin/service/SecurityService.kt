package no.fdk.harvestadmin.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.stereotype.Service
import java.util.regex.Pattern

@Service
class SecurityService(
    @Value("\${app.security.org-type}") private val orgType: String,
) {
    fun getAuthorizedOrganizations(authentication: Authentication?): List<String>? {
        if (authentication == null) {
            return null
        }

        val principal = authentication.principal
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
        val pattern = Pattern.compile("$orgType:([0-9]*):")
        val matcher = pattern.matcher(authorities)
        val orgs = mutableListOf<String>()

        while (matcher.find()) {
            orgs.add(matcher.group(1))
        }

        return orgs
    }
}
