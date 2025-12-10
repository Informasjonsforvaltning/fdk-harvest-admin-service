package no.fdk.harvestadmin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class FdkHarvestAdminServiceApplication

fun main(args: Array<String>) {
    runApplication<FdkHarvestAdminServiceApplication>(*args)
}
