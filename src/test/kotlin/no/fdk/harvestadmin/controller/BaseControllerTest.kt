package no.fdk.harvestadmin.controller

import no.fdk.harvestadmin.service.DataSourceService
import no.fdk.harvestadmin.service.HarvestRunService
import no.fdk.harvestadmin.service.SecurityService
import no.fdk.harvestadmin.service.SysAdminService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc

@WebMvcTest
@ActiveProfiles("test")
@Import(TestSecurityConfig::class)
abstract class BaseControllerTest {
    @Autowired
    protected lateinit var mockMvc: MockMvc

    @MockitoBean
    protected lateinit var dataSourceService: DataSourceService

    @MockitoBean
    protected lateinit var harvestRunService: HarvestRunService

    @MockitoBean
    protected lateinit var securityService: SecurityService

    @MockitoBean
    protected lateinit var sysAdminService: SysAdminService
}
