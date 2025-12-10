package no.fdk.harvestadmin.controller

import no.fdk.harvestadmin.service.DataSourceService
import no.fdk.harvestadmin.service.HarvestRunService
import no.fdk.harvestadmin.service.SecurityService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.MockMvc

/**
 * Base class for controller tests that provides common setup and dependencies.
 *
 * This class eliminates code duplication across controller tests by providing:
 * - WebMvcTest annotation for web layer testing only (no database, no full context)
 * - MockMvc setup for HTTP testing
 * - Mocked services for unit testing using Mockito
 * - Fast execution without external dependencies
 * - Security disabled via application-test.yml
 */
@WebMvcTest
@ActiveProfiles("test")
abstract class BaseControllerTest {
    @Autowired
    protected lateinit var mockMvc: MockMvc

    @MockBean
    protected lateinit var dataSourceService: DataSourceService

    @MockBean
    protected lateinit var harvestRunService: HarvestRunService

    @MockBean
    protected lateinit var securityService: SecurityService
}

