package com.flinkpipeline.payroll.utils;

import com.flinkpipeline.payroll.config.MetricsConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive metrics collection and monitoring system for the payroll data quality pipeline.
 * Provides real-time metrics aggregation, performance monitoring, SLA tracking, and integration
 * with external monitoring systems (Prometheus, Grafana, DataDog).
 *
 * <p>Metrics Categories: - Throughput metrics (records/second, bytes/second) - Latency metrics
 * (processing time, end-to-end latency) - Error metrics (error rates, failure counts by category) -
 * Business metrics (validation success rates, compliance violations) - System metrics (resource
 * utilization, checkpoint performance) - SLA metrics (processing deadlines, quality thresholds)
 */
public class MetricsCollector {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

  // Configuration
  private final MetricsConfig config;
  private final boolean enabled;

  // Metrics storage
  private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
  private final Map<String, DoubleAdder> gauges = new ConcurrentHashMap<>();
  private final Map<String, HistogramMetric> histograms = new ConcurrentHashMap<>();
  private final Map<String, Supplier<Object>> metricSources = new ConcurrentHashMap<>();

  // Timing and aggregation
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
  private volatile Instant startTime = Instant.now();
  private volatile Instant lastReportTime = Instant.now();

  // Business-specific metrics
  private final AtomicLong recordsProcessed = new AtomicLong(0);
  private final AtomicLong validationFailures = new AtomicLong(0);
  private final AtomicLong complianceViolations = new AtomicLong(0);
  private final AtomicLong slaViolations = new AtomicLong(0);
  private final DoubleAdder avgProcessingLatency = new DoubleAdder();
  private final DoubleAdder avgValidationTime = new DoubleAdder();

  // Constructor
  public MetricsCollector(MetricsConfig config) {
    this.config = config;
    this.enabled = config.isEnabled();

    if (enabled) {
      LOG.info(
          "Initializing MetricsCollector with reporter: {}, interval: {}",
          config.getReporter(),
          config.getInterval());
      initializeMetrics();
    } else {
      LOG.info("Metrics collection disabled");
    }
  }

  /** Start metrics collection and reporting */
  public void start() {
    if (!enabled) {
      return;
    }

    LOG.info("Starting metrics collection");

    // Schedule periodic metrics reporting
    scheduler.scheduleAtFixedRate(
        this::reportMetrics,
        config.getInterval().toSeconds(),
        config.getInterval().toSeconds(),
        TimeUnit.SECONDS);

    // Schedule metrics aggregation
    scheduler.scheduleAtFixedRate(
        this::aggregateMetrics,
        10, // Start after 10 seconds
        30, // Every 30 seconds
        TimeUnit.SECONDS);

    LOG.info("Metrics collection started successfully");
  }

  /** Stop metrics collection */
  public void stop() {
    if (!enabled) {
      return;
    }

    LOG.info("Stopping metrics collection");

    try {
      // Final metrics report
      reportMetrics();

      // Shutdown scheduler
      scheduler.shutdown();
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }

      LOG.info("Metrics collection stopped");

    } catch (Exception e) {
      LOG.error("Error stopping metrics collector", e);
    }
  }

  /** Initialize core metrics */
  private void initializeMetrics() {
    // Counter metrics
    registerCounter("records_processed_total");
    registerCounter("validation_failures_total");
    registerCounter("compliance_violations_total");
    registerCounter("sla_violations_total");
    registerCounter("errors_total");
    registerCounter("retries_total");
    registerCounter("dead_letter_records_total");

    // Gauge metrics
    registerGauge("records_per_second");
    registerGauge("validation_success_rate");
    registerGauge("avg_processing_latency_ms");
    registerGauge("avg_validation_time_ms");
    registerGauge("system_health_score");

    // Histogram metrics
    registerHistogram("processing_latency_ms");
    registerHistogram("validation_time_ms");
    registerHistogram("record_size_bytes");
    registerHistogram("batch_size");

    LOG.info("Core metrics initialized");
  }

  /** Register a counter metric */
  public void registerCounter(String name) {
    counters.putIfAbsent(name, new AtomicLong(0));
    LOG.debug("Registered counter: {}", name);
  }

  /** Register a gauge metric */
  public void registerGauge(String name) {
    gauges.putIfAbsent(name, new DoubleAdder());
    LOG.debug("Registered gauge: {}", name);
  }

  /** Register a histogram metric */
  public void registerHistogram(String name) {
    histograms.putIfAbsent(name, new HistogramMetric());
    LOG.debug("Registered histogram: {}", name);
  }

  /** Register external metric source */
  public void registerMetricSource(String name, Supplier<Object> supplier) {
    metricSources.put(name, supplier);
    LOG.debug("Registered metric source: {}", name);
  }

  /** Increment a counter */
  public void incrementCounter(String name) {
    incrementCounter(name, 1);
  }

  /** Increment a counter by specific amount */
  public void incrementCounter(String name, long amount) {
    if (!enabled) return;

    AtomicLong counter = counters.get(name);
    if (counter != null) {
      counter.addAndGet(amount);
    } else {
      LOG.warn("Unknown counter: {}", name);
    }
  }

  /** Set gauge value */
  public void setGauge(String name, double value) {
    if (!enabled) return;

    DoubleAdder gauge = gauges.get(name);
    if (gauge != null) {
      gauge.reset();
      gauge.add(value);
    } else {
      LOG.warn("Unknown gauge: {}", name);
    }
  }

  /** Record histogram value */
  public void recordHistogram(String name, double value) {
    if (!enabled) return;

    HistogramMetric histogram = histograms.get(name);
    if (histogram != null) {
      histogram.record(value);
    } else {
      LOG.warn("Unknown histogram: {}", name);
    }
  }

  /** Record processing metrics */
  public void recordProcessing(
      long processingTimeMs, boolean validationSuccess, boolean complianceViolation) {
    if (!enabled) return;

    recordsProcessed.incrementAndGet();
    incrementCounter("records_processed_total");

    recordHistogram("processing_latency_ms", processingTimeMs);
    avgProcessingLatency.add(processingTimeMs);

    if (!validationSuccess) {
      validationFailures.incrementAndGet();
      incrementCounter("validation_failures_total");
    }

    if (complianceViolation) {
      complianceViolations.incrementAndGet();
      incrementCounter("compliance_violations_total");
    }
  }

  /** Record validation metrics */
  public void recordValidation(long validationTimeMs, int rulesPassed, int rulesFailed) {
    if (!enabled) return;

    recordHistogram("validation_time_ms", validationTimeMs);
    avgValidationTime.add(validationTimeMs);

    incrementCounter("validation_rules_passed_total", rulesPassed);
    incrementCounter("validation_rules_failed_total", rulesFailed);
  }

  /** Record SLA violation */
  public void recordSlaViolation(String slaType, Duration violationAmount) {
    if (!enabled) return;

    slaViolations.incrementAndGet();
    incrementCounter("sla_violations_total");
    incrementCounter("sla_violations_" + slaType.toLowerCase());

    recordHistogram("sla_violation_amount_ms", violationAmount.toMillis());
  }

  /** Record error metrics */
  public void recordError(String errorType, String errorCategory) {
    if (!enabled) return;

    incrementCounter("errors_total");
    incrementCounter("errors_" + errorType.toLowerCase());
    incrementCounter("errors_category_" + errorCategory.toLowerCase());
  }

  /** Aggregate and calculate derived metrics */
  private void aggregateMetrics() {
    if (!enabled) return;

    try {
      Instant now = Instant.now();
      Duration timeSinceLastReport = Duration.between(lastReportTime, now);

      // Calculate throughput metrics
      long totalRecords = recordsProcessed.get();
      double recordsPerSecond =
          totalRecords / Math.max(1, Duration.between(startTime, now).getSeconds());
      setGauge("records_per_second", recordsPerSecond);

      // Calculate success rates
      long totalFailures = validationFailures.get();
      double successRate =
          totalRecords > 0 ? (double) (totalRecords - totalFailures) / totalRecords * 100 : 100.0;
      setGauge("validation_success_rate", successRate);

      // Calculate average latencies
      if (totalRecords > 0) {
        setGauge("avg_processing_latency_ms", avgProcessingLatency.sum() / totalRecords);
        setGauge("avg_validation_time_ms", avgValidationTime.sum() / totalRecords);
      }

      // Calculate system health score
      double healthScore = calculateSystemHealthScore();
      setGauge("system_health_score", healthScore);

      // Update external metric sources
      updateExternalMetrics();

      lastReportTime = now;

    } catch (Exception e) {
      LOG.error("Error aggregating metrics", e);
    }
  }

  /** Calculate overall system health score (0-100) */
  private double calculateSystemHealthScore() {
    double score = 100.0;

    // Penalize for high error rates
    long totalRecords = recordsProcessed.get();
    if (totalRecords > 0) {
      double errorRate = (double) validationFailures.get() / totalRecords;
      score -= errorRate * 50; // Up to 50 point penalty for 100% error rate
    }

    // Penalize for compliance violations
    if (totalRecords > 0) {
      double complianceRate = (double) complianceViolations.get() / totalRecords;
      score -= complianceRate * 30; // Up to 30 point penalty for compliance issues
    }

    // Penalize for SLA violations
    if (slaViolations.get() > 0) {
      score -= Math.min(20, slaViolations.get()); // Up to 20 point penalty
    }

    return Math.max(0, score);
  }

  /** Update metrics from external sources */
  private void updateExternalMetrics() {
    metricSources.forEach(
        (name, supplier) -> {
          try {
            Object value = supplier.get();
            if (value instanceof Number) {
              setGauge("external_" + name, ((Number) value).doubleValue());
            }
          } catch (Exception e) {
            LOG.warn("Failed to update external metric: {}", name, e);
          }
        });
  }

  /** Report all metrics */
  private void reportMetrics() {
    if (!enabled) return;

    try {
      Map<String, Object> allMetrics = getAllMetrics();

      switch (config.getReporter().toLowerCase()) {
        case "slf4j":
          reportToSlf4j(allMetrics);
          break;
        case "prometheus":
          reportToPrometheus(allMetrics);
          break;
        case "datadog":
          reportToDatadog(allMetrics);
          break;
        case "console":
          reportToConsole(allMetrics);
          break;
        default:
          LOG.warn("Unknown metrics reporter: {}", config.getReporter());
          reportToSlf4j(allMetrics);
      }

    } catch (Exception e) {
      LOG.error("Error reporting metrics", e);
    }
  }

  /** Get all current metrics */
  public Map<String, Object> getAllMetrics() {
    Map<String, Object> allMetrics = new HashMap<>();

    // Add counters
    counters.forEach((name, counter) -> allMetrics.put(name, counter.get()));

    // Add gauges
    gauges.forEach((name, gauge) -> allMetrics.put(name, gauge.sum()));

    // Add histogram summaries
    histograms.forEach(
        (name, histogram) -> {
          Map<String, Double> histogramData = histogram.getSummary();
          histogramData.forEach((metric, value) -> allMetrics.put(name + "_" + metric, value));
        });

    // Add derived metrics
    allMetrics.put("uptime_seconds", Duration.between(startTime, Instant.now()).getSeconds());
    allMetrics.put("total_records_processed", recordsProcessed.get());
    allMetrics.put("total_validation_failures", validationFailures.get());
    allMetrics.put("total_compliance_violations", complianceViolations.get());
    allMetrics.put("total_sla_violations", slaViolations.get());

    return allMetrics;
  }

  /** Report metrics to SLF4J logger */
  private void reportToSlf4j(Map<String, Object> metrics) {
    StringBuilder report = new StringBuilder("\n=== Payroll Pipeline Metrics ===\n");

    // Business metrics
    report.append("Business Metrics:\n");
    report.append(String.format("  Records Processed: %d\n", recordsProcessed.get()));
    report.append(
        String.format(
            "  Validation Success Rate: %.2f%%\n", gauges.get("validation_success_rate").sum()));
    report.append(String.format("  Compliance Violations: %d\n", complianceViolations.get()));
    report.append(String.format("  SLA Violations: %d\n", slaViolations.get()));

    // Performance metrics
    report.append("Performance Metrics:\n");
    report.append(
        String.format("  Records/Second: %.2f\n", gauges.get("records_per_second").sum()));
    report.append(
        String.format(
            "  Avg Processing Latency: %.2f ms\n", gauges.get("avg_processing_latency_ms").sum()));
    report.append(
        String.format("  System Health Score: %.1f\n", gauges.get("system_health_score").sum()));

    // Error metrics
    report.append("Error Metrics:\n");
    report.append(
        String.format(
            "  Total Errors: %d\n",
            counters.getOrDefault("errors_total", new AtomicLong(0)).get()));
    report.append(
        String.format(
            "  Retries Attempted: %d\n",
            counters.getOrDefault("retries_total", new AtomicLong(0)).get()));

    report.append("================================");

    LOG.info(report.toString());
  }

  /** Report metrics to Prometheus (placeholder implementation) */
  private void reportToPrometheus(Map<String, Object> metrics) {
    // In real implementation, would push to Prometheus pushgateway or expose endpoint
    LOG.debug(
        "Reporting {} metrics to Prometheus endpoint: {}", metrics.size(), config.getEndpoint());
  }

  /** Report metrics to DataDog (placeholder implementation) */
  private void reportToDatadog(Map<String, Object> metrics) {
    // In real implementation, would use DataDog client
    LOG.debug("Reporting {} metrics to DataDog", metrics.size());
  }

  /** Report metrics to console */
  private void reportToConsole(Map<String, Object> metrics) {
    System.out.println("\n=== Payroll Pipeline Metrics ===");
    metrics.forEach((key, value) -> System.out.printf("%s: %s%n", key, value));
    System.out.println("================================\n");
  }

  /** Get overall metrics summary */
  public Object getOverallMetrics() {
    return new MetricsSummary(
        recordsProcessed.get(),
        validationFailures.get(),
        complianceViolations.get(),
        slaViolations.get(),
        gauges.get("validation_success_rate").sum(),
        gauges.get("system_health_score").sum(),
        Duration.between(startTime, Instant.now()));
  }

  /** Reset all metrics (useful for testing) */
  public void reset() {
    counters.values().forEach(counter -> counter.set(0));
    gauges.values().forEach(gauge -> gauge.reset());
    histograms.values().forEach(histogram -> histogram.reset());

    recordsProcessed.set(0);
    validationFailures.set(0);
    complianceViolations.set(0);
    slaViolations.set(0);
    avgProcessingLatency.reset();
    avgValidationTime.reset();

    startTime = Instant.now();
    lastReportTime = Instant.now();
  }

  /** Histogram metric implementation */
  private static class HistogramMetric {
    private final AtomicLong count = new AtomicLong(0);
    private final DoubleAdder sum = new DoubleAdder();
    private volatile double min = Double.MAX_VALUE;
    private volatile double max = Double.MIN_VALUE;

    public synchronized void record(double value) {
      count.incrementAndGet();
      sum.add(value);
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

    public Map<String, Double> getSummary() {
      long totalCount = count.get();
      Map<String, Double> summary = new HashMap<>();

      summary.put("count", (double) totalCount);
      summary.put("sum", sum.sum());
      summary.put("avg", totalCount > 0 ? sum.sum() / totalCount : 0.0);
      summary.put("min", min == Double.MAX_VALUE ? 0.0 : min);
      summary.put("max", max == Double.MIN_VALUE ? 0.0 : max);

      return summary;
    }

    public void reset() {
      count.set(0);
      sum.reset();
      min = Double.MAX_VALUE;
      max = Double.MIN_VALUE;
    }
  }

  /** Metrics summary data class */
  public static class MetricsSummary {
    private final long totalRecords;
    private final long validationFailures;
    private final long complianceViolations;
    private final long slaViolations;
    private final double successRate;
    private final double healthScore;
    private final Duration uptime;

    public MetricsSummary(
        long totalRecords,
        long validationFailures,
        long complianceViolations,
        long slaViolations,
        double successRate,
        double healthScore,
        Duration uptime) {
      this.totalRecords = totalRecords;
      this.validationFailures = validationFailures;
      this.complianceViolations = complianceViolations;
      this.slaViolations = slaViolations;
      this.successRate = successRate;
      this.healthScore = healthScore;
      this.uptime = uptime;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getValidationFailures() {
      return validationFailures;
    }

    public long getComplianceViolations() {
      return complianceViolations;
    }

    public long getSlaViolations() {
      return slaViolations;
    }

    public double getSuccessRate() {
      return successRate;
    }

    public double getHealthScore() {
      return healthScore;
    }

    public Duration getUptime() {
      return uptime;
    }

    @Override
    public String toString() {
      return String.format(
          "MetricsSummary{records=%d, failures=%d, violations=%d, slaViolations=%d, successRate=%.2f%%, healthScore=%.1f, uptime=%s}",
          totalRecords,
          validationFailures,
          complianceViolations,
          slaViolations,
          successRate,
          healthScore,
          uptime);
    }
  }
}
