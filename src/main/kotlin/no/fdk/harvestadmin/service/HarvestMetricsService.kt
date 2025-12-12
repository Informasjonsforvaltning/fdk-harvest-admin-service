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
    // Normalize dataType to lowercase for consistent Prometheus labels
    private fun normalizeDataType(dataType: String): String {
        // Convert to lowercase and handle special cases
        return when (dataType.lowercase()) {
            "concept" -> "concept"
            "dataset" -> "dataset"
            "informationmodel", "information_model" -> "informationmodel"
            "dataservice", "data_service" -> "dataservice"
            "publicservice", "public_service" -> "publicservice"
            "event" -> "event"
            else -> dataType.lowercase() // Fallback to lowercase
        }
    }

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

    // Timers for phase durations (records in seconds for Prometheus)
    private val phaseDurationTimer: Timer.Builder =
        Timer
            .builder("harvest.phase.duration")
            .description("Duration of harvest phases in seconds")
            .publishPercentileHistogram()

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

    private val partiallyProcessedResourcesHistogram: io.micrometer.core.instrument.DistributionSummary.Builder =
        io.micrometer.core.instrument.DistributionSummary
            .builder("harvest.run.resources.partially_processed")
            .description(
                "Number of partially processed resources per run (resources that have completed at least one phase but not all phases)",
            ).baseUnit("resources")

    // Gauges for current runs and progress - registered at startup
    @PostConstruct
    fun registerCurrentRunsGauge() {
        Gauge
            .builder("harvest.runs.current") {
                harvestRunRepository.findAllInProgress().size.toDouble()
            }.description("Current number of in-progress harvest runs")
            .register(meterRegistry)

        // Register gauge for total processed resources across all in-progress runs
        Gauge
            .builder("harvest.runs.processed_resources") {
                harvestRunRepository
                    .findAllInProgress()
                    .sumOf { it.processedResources?.toLong() ?: 0L }
                    .toDouble()
            }.description("Total processed resources across all in-progress runs")
            .register(meterRegistry)

        // Register gauge for total resources across all in-progress runs
        Gauge
            .builder("harvest.runs.total_resources") {
                harvestRunRepository
                    .findAllInProgress()
                    .sumOf { it.totalResources?.toLong() ?: 0L }
                    .toDouble()
            }.description("Total resources across all in-progress runs")
            .register(meterRegistry)

        // Register gauge for processed resources from completed runs (last 24 hours)
        Gauge
            .builder("harvest.runs.completed.processed_resources") {
                val oneDayAgo =
                    java.time.Instant
                        .now()
                        .minus(24, java.time.temporal.ChronoUnit.HOURS)
                harvestRunRepository
                    .findAllCompletedRuns(oneDayAgo)
                    .sumOf { it.processedResources?.toLong() ?: 0L }
                    .toDouble()
            }.description("Total processed resources from completed runs in last 24 hours")
            .register(meterRegistry)

        // Register gauge for total processed resources (in-progress + completed in last 24h)
        Gauge
            .builder("harvest.runs.all.processed_resources") {
                val inProgress =
                    harvestRunRepository
                        .findAllInProgress()
                        .sumOf { it.processedResources?.toLong() ?: 0L }
                val oneDayAgo =
                    java.time.Instant
                        .now()
                        .minus(24, java.time.temporal.ChronoUnit.HOURS)
                val completed =
                    harvestRunRepository
                        .findAllCompletedRuns(oneDayAgo)
                        .sumOf { it.processedResources?.toLong() ?: 0L }
                (inProgress + completed).toDouble()
            }.description("Total processed resources (in-progress + completed in last 24h)")
            .register(meterRegistry)

        // Initialize DistributionSummary metrics to ensure they're always exposed
        // This ensures harvest_run_resources_total_sum and harvest_run_resources_processed_sum
        // are available in Prometheus even before any data is recorded
        val dataTypes = listOf("concept", "dataset", "informationmodel", "dataservice", "publicservice", "event")
        dataTypes.forEach { dataType ->
            // Initialize totalResourcesHistogram with 0 to ensure metric exists
            totalResourcesHistogram
                .tag("datatype", dataType)
                .register(meterRegistry)
                .record(0.0)

            // Initialize processedResourcesHistogram with 0 to ensure metric exists
            processedResourcesHistogram
                .tag("datatype", dataType)
                .register(meterRegistry)
                .record(0.0)

            // Initialize partiallyProcessedResourcesHistogram with 0 to ensure metric exists
            partiallyProcessedResourcesHistogram
                .tag("datatype", dataType)
                .register(meterRegistry)
                .record(0.0)
        }
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
            .tag("datatype", normalizeDataType(event.dataType.name))
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

                // Convert milliseconds to Duration for Timer
                val duration = java.time.Duration.ofMillis(durationMs)
                phaseDurationTimer
                    .tag("phase", event.phase.name)
                    .tag("datatype", normalizeDataType(event.dataType.name))
                    .register(meterRegistry)
                    .record(duration)
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

            // Record total duration (convert ms to Duration)
            if (run.runStartedAt != null && run.totalDurationMs != null) {
                val totalDuration = java.time.Duration.ofMillis(run.totalDurationMs!!)
                phaseDurationTimer
                    .tag("phase", "TOTAL")
                    .tag("datatype", normalizeDataType(run.dataType))
                    .register(meterRegistry)
                    .record(totalDuration)
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
                    .tag("datatype", normalizeDataType(run.dataType))
                    .register(meterRegistry)
                    .record(run.totalResources.toDouble())
            }

            if (run.processedResources != null && run.processedResources > 0) {
                processedResourcesHistogram
                    .tag("datatype", normalizeDataType(run.dataType))
                    .register(meterRegistry)
                    .record(run.processedResources.toDouble())
            }

            // Record partially processed resources (resources that have completed at least one phase)
            if (run.partiallyProcessedResources != null) {
                partiallyProcessedResourcesHistogram
                    .tag("datatype", normalizeDataType(run.dataType))
                    .register(meterRegistry)
                    .record(run.partiallyProcessedResources.toDouble())
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
            .tag("datatype", normalizeDataType(dataType))
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
            // Convert milliseconds to Duration for Timer
            val duration = java.time.Duration.ofMillis(durationMs)
            phaseDurationTimer
                .tag("phase", phase)
                .tag("datatype", normalizeDataType(dataType))
                .register(meterRegistry)
                .record(duration)
        }
    }

    // Record phase duration during run (when phase completes)
    fun recordPhaseDurationDuringRun(
        phase: String,
        durationMs: Long,
        dataType: String,
    ) {
        val duration = java.time.Duration.ofMillis(durationMs)
        phaseDurationTimer
            .tag("phase", phase)
            .tag("datatype", normalizeDataType(dataType))
            .register(meterRegistry)
            .record(duration)
    }

    // Record resource counts during run (for in-progress runs)
    fun recordRunResourceCounts(run: HarvestRunEntity) {
        if (run.totalResources != null && run.totalResources > 0) {
            totalResourcesHistogram
                .tag("datatype", normalizeDataType(run.dataType))
                .register(meterRegistry)
                .record(run.totalResources.toDouble())
        }

        if (run.processedResources != null && run.processedResources > 0) {
            processedResourcesHistogram
                .tag("datatype", normalizeDataType(run.dataType))
                .register(meterRegistry)
                .record(run.processedResources.toDouble())
        }

        // Record partially processed resources (resources that have completed at least one phase) during run
        if (run.partiallyProcessedResources != null) {
            partiallyProcessedResourcesHistogram
                .tag("datatype", normalizeDataType(run.dataType))
                .register(meterRegistry)
                .record(run.partiallyProcessedResources.toDouble())
        }
    }
}
