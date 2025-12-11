package no.fdk.harvestadmin.service

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PostConstruct
import no.fdk.harvest.HarvestEvent
import no.fdk.harvestadmin.entity.HarvestRunEntity
import no.fdk.harvestadmin.repository.HarvestRunRepository
import org.springframework.stereotype.Service

@Service
class HarvestMetricsService(
    private val meterRegistry: MeterRegistry,
    private val harvestRunRepository: HarvestRunRepository,
) {
    // Counters for harvest events
    private val eventsProcessedCounter: Counter =
        Counter
            .builder("harvest.events.processed")
            .description("Total number of harvest events processed")
            .tag("type", "total")
            .register(meterRegistry)

    private val eventsByPhaseCounter: Counter.Builder =
        Counter
            .builder("harvest.events.by.phase")
            .description("Number of harvest events by phase")

    private val eventsByDataTypeCounter: Counter.Builder =
        Counter
            .builder("harvest.events.by.datatype")
            .description("Number of harvest events by data type")

    // Counters for harvest runs
    private val runsStartedCounter: Counter =
        Counter
            .builder("harvest.runs.started")
            .description("Total number of harvest runs started")
            .register(meterRegistry)

    private val runsCompletedCounter: Counter =
        Counter
            .builder("harvest.runs.completed")
            .description("Total number of harvest runs completed")
            .register(meterRegistry)

    private val runsFailedCounter: Counter =
        Counter
            .builder("harvest.runs.failed")
            .description("Total number of harvest runs failed")
            .register(meterRegistry)

    // Timers for phase durations
    private val phaseDurationTimer: Timer.Builder =
        Timer
            .builder("harvest.phase.duration")
            .description("Duration of harvest phases in milliseconds")

    // Resource processing metrics
    private val resourcesProcessedCounter: Counter.Builder =
        Counter
            .builder("harvest.resources.processed")
            .description("Number of resources processed")

    // Resource count metrics (histograms for per-run values)
    private val totalResourcesHistogram: io.micrometer.core.instrument.DistributionSummary.Builder =
        io.micrometer.core.instrument.DistributionSummary
            .builder("harvest.run.resources.total")
            .description("Total number of resources per run")
            .baseUnit("resources")

    private val processedResourcesHistogram: io.micrometer.core.instrument.DistributionSummary.Builder =
        io.micrometer.core.instrument.DistributionSummary
            .builder("harvest.run.resources.processed")
            .description("Number of processed resources per run")
            .baseUnit("resources")

    // Gauge for current runs - registered at startup
    @PostConstruct
    fun registerCurrentRunsGauge() {
        Gauge
            .builder("harvest.runs.current") {
                harvestRunRepository.findAllInProgress().size.toDouble()
            }.description("Current number of in-progress harvest runs")
            .register(meterRegistry)
    }

    fun recordEventProcessed(event: HarvestEvent) {
        eventsProcessedCounter.increment()

        // Record event by phase
        eventsByPhaseCounter
            .tag("phase", event.phase.name)
            .register(meterRegistry)
            .increment()

        // Record event by data type
        eventsByDataTypeCounter
            .tag("datatype", event.dataType.name)
            .register(meterRegistry)
            .increment()

        // Record phase duration if endTime is available
        if (event.startTime != null && event.endTime != null) {
            try {
                val start = java.time.Instant.parse(event.startTime.toString())
                val end = java.time.Instant.parse(event.endTime.toString())
                val durationMs =
                    java.time.Duration
                        .between(start, end)
                        .toMillis()

                phaseDurationTimer
                    .tag("phase", event.phase.name)
                    .tag("datatype", event.dataType.name)
                    .register(meterRegistry)
                    .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                // Ignore parsing errors
            }
        }
    }

    fun recordRunStarted(run: HarvestRunEntity) {
        runsStartedCounter.increment()
    }

    fun recordRunCompleted(run: HarvestRunEntity) {
        if (run.status == "COMPLETED") {
            runsCompletedCounter.increment()

            // Record total duration
            if (run.runStartedAt != null && run.totalDurationMs != null) {
                phaseDurationTimer
                    .tag("phase", "TOTAL")
                    .tag("datatype", run.dataType)
                    .register(meterRegistry)
                    .record(run.totalDurationMs!!, java.util.concurrent.TimeUnit.MILLISECONDS)
            }

            // Record individual phase durations
            recordPhaseDuration("INITIATING", run.initDurationMs, run.dataType)
            recordPhaseDuration("HARVESTING", run.harvestDurationMs, run.dataType)
            recordPhaseDuration("REASONING", run.reasoningDurationMs, run.dataType)
            recordPhaseDuration("RDF_PARSING", run.rdfParsingDurationMs, run.dataType)
            recordPhaseDuration("SEARCH_PROCESSING", run.searchProcessingDurationMs, run.dataType)
            recordPhaseDuration("AI_SEARCH_PROCESSING", run.aiSearchProcessingDurationMs, run.dataType)
            recordPhaseDuration("RESOURCE_PROCESSING", run.apiProcessingDurationMs, run.dataType)
            recordPhaseDuration("SPARQL_PROCESSING", run.sparqlProcessingDurationMs, run.dataType)

            // Record resource counts
            if (run.totalResources != null && run.totalResources > 0) {
                totalResourcesHistogram
                    .tag("datatype", run.dataType)
                    .register(meterRegistry)
                    .record(run.totalResources.toDouble())
            }

            if (run.processedResources != null && run.processedResources > 0) {
                processedResourcesHistogram
                    .tag("datatype", run.dataType)
                    .register(meterRegistry)
                    .record(run.processedResources.toDouble())
            }
        } else if (run.status == "FAILED") {
            runsFailedCounter.increment()
        }
    }

    fun recordResourcesProcessed(
        dataType: String,
        phase: String,
        count: Int,
    ) {
        resourcesProcessedCounter
            .tag("datatype", dataType)
            .tag("phase", phase)
            .register(meterRegistry)
            .increment(count.toDouble())
    }

    private fun recordPhaseDuration(
        phase: String,
        durationMs: Long?,
        dataType: String,
    ) {
        if (durationMs != null && durationMs > 0) {
            phaseDurationTimer
                .tag("phase", phase)
                .tag("datatype", dataType)
                .register(meterRegistry)
                .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS)
        }
    }
}
