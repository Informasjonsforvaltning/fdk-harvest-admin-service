package no.fdk.harvestadmin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class FdkHarvestAdminServiceApplication

fun main(args: Array<String>) {
    runApplication<FdkHarvestAdminServiceApplication>(*args)
}
