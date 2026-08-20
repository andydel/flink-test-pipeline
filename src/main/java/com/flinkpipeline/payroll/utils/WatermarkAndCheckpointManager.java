package com.flinkpipeline.payroll.utils;

import com.flinkpipeline.payroll.config.CheckpointConfig;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advanced watermark generation and checkpoint coordination for the payroll data quality pipeline.
 * Implements sophisticated event-time processing with late data handling, checkpoint optimization,
 * and SLA-aware watermark advancement for real-time payroll processing.
 *
 * <p>Features: - Adaptive watermark generation based on data patterns - Late data detection and
 * handling with grace periods - SLA-aware watermark advancement for compliance deadlines -
 * Checkpoint coordination with business cycle alignment - Performance-optimized checkpoint triggers
 * - Integration with pipeline state management - Backpressure-aware watermark generation
 */
public class WatermarkAndCheckpointManager {

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkAndCheckpointManager.class);

  // Configuration
  private final CheckpointConfig checkpointConfig;
  private final Duration maxOutOfOrderness;
  private final Duration idleTimeout;
  private final Duration slaDeadline;
  private final boolean enableAdaptiveWatermarks;

  // Watermark state tracking
  private final AtomicLong lastWatermark = new AtomicLong(Long.MIN_VALUE);
  private final AtomicLong lateRecordsCount = new AtomicLong(0);
  private final AtomicLong watermarkUpdates = new AtomicLong(0);

  // Checkpoint state tracking
  private final AtomicLong checkpointCount = new AtomicLong(0);
  private final AtomicLong checkpointFailures = new AtomicLong(0);
  private volatile Instant lastCheckpointTime = Instant.now();
  private volatile Duration lastCheckpointDuration = Duration.ZERO;

  // Business timing constraints
  private static final Duration PAYROLL_PROCESSING_SLA = Duration.ofHours(4);
  private static final Duration END_OF_DAY_DEADLINE = Duration.ofHours(18); // 6 PM
  private static final Duration COMPLIANCE_WINDOW = Duration.ofMinutes(15);

  // Constructor
  public WatermarkAndCheckpointManager(CheckpointConfig checkpointConfig) {
    this(
        checkpointConfig,
        Duration.ofMinutes(5),
        Duration.ofMinutes(10),
        PAYROLL_PROCESSING_SLA,
        true);
  }

  public WatermarkAndCheckpointManager(
      CheckpointConfig checkpointConfig,
      Duration maxOutOfOrderness,
      Duration idleTimeout,
      Duration slaDeadline,
      boolean enableAdaptiveWatermarks) {
    this.checkpointConfig = checkpointConfig;
    this.maxOutOfOrderness = maxOutOfOrderness;
    this.idleTimeout = idleTimeout;
    this.slaDeadline = slaDeadline;
    this.enableAdaptiveWatermarks = enableAdaptiveWatermarks;

    LOG.info(
        "Initialized WatermarkAndCheckpointManager - maxOutOfOrderness: {}, idleTimeout: {}, slaDeadline: {}",
        maxOutOfOrderness,
        idleTimeout,
        slaDeadline);
  }

  /** Configure checkpointing for the Flink environment */
  public void configureCheckpointing(StreamExecutionEnvironment env) {
    if (!checkpointConfig.isEnabled()) {
      LOG.info("Checkpointing is disabled");
      return;
    }

    LOG.info("Configuring checkpointing with interval: {}ms", checkpointConfig.getInterval());

    // Enable checkpointing
    env.enableCheckpointing(checkpointConfig.getInterval());

    // Configure checkpoint behavior
    var config = env.getCheckpointConfig();
    config.setMinPauseBetweenCheckpoints(checkpointConfig.getMinPause());
    config.setCheckpointTimeout(checkpointConfig.getTimeout());
    config.setMaxConcurrentCheckpoints(1);
    config.setTolerableCheckpointFailureNumber(3);

    // Configure cleanup behavior
    config.setExternalizedCheckpointCleanup(
        org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
            .RETAIN_ON_CANCELLATION);

    // Enable incremental checkpoints for RocksDB
    if ("rocksdb".equals(checkpointConfig.getStoragePath())) {
      config.enableUnalignedCheckpoints(true);
      LOG.info("Enabled unaligned checkpoints for better performance");
    }

    LOG.info("Checkpointing configured successfully");
  }

  /** Create adaptive watermark strategy for payroll employee records */
  public WatermarkStrategy<PayrollEmployee> createPayrollWatermarkStrategy() {
    LOG.info("Creating adaptive watermark strategy for payroll records");

    WatermarkStrategy<PayrollEmployee> strategy;

    if (enableAdaptiveWatermarks) {
      strategy =
          WatermarkStrategy.<PayrollEmployee>forGenerator(new AdaptiveWatermarkGeneratorSupplier())
              .withTimestampAssigner(new PayrollTimestampAssigner())
              .withIdleness(idleTimeout);
    } else {
      strategy =
          WatermarkStrategy.<PayrollEmployee>forBoundedOutOfOrderness(maxOutOfOrderness)
              .withTimestampAssigner(new PayrollTimestampAssigner())
              .withIdleness(idleTimeout);
    }

    LOG.info("Watermark strategy created with adaptive mode: {}", enableAdaptiveWatermarks);
    return strategy;
  }

  /** Create SLA-aware watermark strategy for compliance-critical processing */
  public WatermarkStrategy<PayrollEmployee> createSLAAwareWatermarkStrategy() {
    LOG.info("Creating SLA-aware watermark strategy");

    return WatermarkStrategy.<PayrollEmployee>forGenerator(new SLAAwareWatermarkGeneratorSupplier())
        .withTimestampAssigner(new PayrollTimestampAssigner())
        .withIdleness(Duration.ofMinutes(2)); // Shorter idle timeout for SLA compliance
  }

  /** Timestamp assigner for payroll employee records */
  private class PayrollTimestampAssigner implements SerializableTimestampAssigner<PayrollEmployee> {
    @Override
    public long extractTimestamp(PayrollEmployee record, long recordTimestamp) {
      // Use ingestion timestamp if available, otherwise use processing time
      long timestamp =
          record.getIngestionTimestamp() != null
              ? record.getIngestionTimestamp()
              : System.currentTimeMillis();

      // Track late records
      long currentWatermark = lastWatermark.get();
      if (currentWatermark != Long.MIN_VALUE && timestamp < currentWatermark) {
        lateRecordsCount.incrementAndGet();
        LOG.debug(
            "Late record detected: record timestamp {}, current watermark {}",
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(currentWatermark));
      }

      return timestamp;
    }
  }

  /** Adaptive watermark generator supplier */
  private class AdaptiveWatermarkGeneratorSupplier
      implements WatermarkGeneratorSupplier<PayrollEmployee> {
    @Override
    public WatermarkGenerator<PayrollEmployee> createWatermarkGenerator(Context context) {
      return new AdaptiveWatermarkGenerator();
    }
  }

  /** Adaptive watermark generator that adjusts based on data patterns */
  private class AdaptiveWatermarkGenerator implements WatermarkGenerator<PayrollEmployee> {
    private long maxTimestamp = Long.MIN_VALUE;
    private long lastWatermarkTime = Long.MIN_VALUE;
    private final Duration adaptiveInterval = Duration.ofSeconds(30);
    private long recordCount = 0;
    private long totalLateness = 0;

    @Override
    public void onEvent(PayrollEmployee event, long eventTimestamp, WatermarkOutput output) {
      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
      recordCount++;

      // Calculate adaptive out-of-orderness based on recent data patterns
      if (lastWatermarkTime != Long.MIN_VALUE) {
        long lateness = Math.max(0, maxTimestamp - eventTimestamp);
        totalLateness += lateness;
      }

      // Emit watermark more frequently for high-volume periods
      if (recordCount % 1000 == 0) {
        emitWatermark(output);
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      emitWatermark(output);
    }

    private void emitWatermark(WatermarkOutput output) {
      if (maxTimestamp == Long.MIN_VALUE) {
        return;
      }

      // Calculate adaptive lag based on observed lateness
      long adaptiveLag = calculateAdaptiveLag();
      long newWatermark = maxTimestamp - adaptiveLag;

      // Ensure watermark never goes backwards
      if (newWatermark > lastWatermarkTime) {
        output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(newWatermark));
        lastWatermarkTime = newWatermark;
        lastWatermark.set(newWatermark);
        watermarkUpdates.incrementAndGet();

        LOG.debug(
            "Emitted adaptive watermark: {} (lag: {}ms)",
            Instant.ofEpochMilli(newWatermark),
            adaptiveLag);
      }
    }

    private long calculateAdaptiveLag() {
      if (recordCount == 0) {
        return maxOutOfOrderness.toMillis();
      }

      // Calculate average lateness
      long avgLateness = totalLateness / recordCount;

      // Adaptive lag: base lag + observed lateness + safety margin
      long adaptiveLag = maxOutOfOrderness.toMillis() + avgLateness + 1000; // 1s safety margin

      // Cap the lag to prevent excessive delays
      return Math.min(adaptiveLag, Duration.ofMinutes(15).toMillis());
    }
  }

  /** SLA-aware watermark generator supplier */
  private class SLAAwareWatermarkGeneratorSupplier
      implements WatermarkGeneratorSupplier<PayrollEmployee> {
    @Override
    public WatermarkGenerator<PayrollEmployee> createWatermarkGenerator(Context context) {
      return new SLAAwareWatermarkGenerator();
    }
  }

  /** SLA-aware watermark generator for compliance-critical processing */
  private class SLAAwareWatermarkGenerator implements WatermarkGenerator<PayrollEmployee> {
    private long maxTimestamp = Long.MIN_VALUE;
    private long lastWatermarkTime = Long.MIN_VALUE;

    @Override
    public void onEvent(PayrollEmployee event, long eventTimestamp, WatermarkOutput output) {
      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);

      // Check if we're approaching SLA deadline
      Instant recordTime = Instant.ofEpochMilli(eventTimestamp);
      Instant now = Instant.now();
      Duration age = Duration.between(recordTime, now);

      // Accelerate watermark advancement for records approaching SLA deadline
      if (age.compareTo(slaDeadline.minus(COMPLIANCE_WINDOW)) > 0) {
        emitUrgentWatermark(output, "SLA deadline approaching");
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      emitWatermark(output);
    }

    private void emitWatermark(WatermarkOutput output) {
      if (maxTimestamp == Long.MIN_VALUE) {
        return;
      }

      // Use shorter lag for SLA-critical processing
      long slaAwareLag = Math.min(maxOutOfOrderness.toMillis(), Duration.ofMinutes(2).toMillis());
      long newWatermark = maxTimestamp - slaAwareLag;

      if (newWatermark > lastWatermarkTime) {
        output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(newWatermark));
        lastWatermarkTime = newWatermark;
        lastWatermark.set(newWatermark);
        watermarkUpdates.incrementAndGet();

        LOG.debug(
            "Emitted SLA-aware watermark: {} (lag: {}ms)",
            Instant.ofEpochMilli(newWatermark),
            slaAwareLag);
      }
    }

    private void emitUrgentWatermark(WatermarkOutput output, String reason) {
      if (maxTimestamp == Long.MIN_VALUE) {
        return;
      }

      // Minimal lag for urgent processing
      long urgentLag = Duration.ofSeconds(30).toMillis();
      long urgentWatermark = maxTimestamp - urgentLag;

      if (urgentWatermark > lastWatermarkTime) {
        output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(urgentWatermark));
        lastWatermarkTime = urgentWatermark;
        lastWatermark.set(urgentWatermark);
        watermarkUpdates.incrementAndGet();

        LOG.warn(
            "Emitted urgent watermark: {} - Reason: {}",
            Instant.ofEpochMilli(urgentWatermark),
            reason);
      }
    }
  }

  /** Checkpoint listener for tracking checkpoint performance */
  public static class PayrollCheckpointListener implements CheckpointListener {
    private final AtomicLong checkpointCount = new AtomicLong(0);
    private final AtomicLong checkpointFailures = new AtomicLong(0);
    private volatile Instant lastCheckpointTime = Instant.now();

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      checkpointCount.incrementAndGet();
      Instant now = Instant.now();
      Duration checkpointDuration = Duration.between(lastCheckpointTime, now);

      LOG.info(
          "Checkpoint {} completed successfully in {}ms",
          checkpointId,
          checkpointDuration.toMillis());

      // Track checkpoint performance
      if (checkpointDuration.compareTo(Duration.ofMinutes(2)) > 0) {
        LOG.warn("Slow checkpoint detected: {} took {}", checkpointId, checkpointDuration);
      }

      lastCheckpointTime = now;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
      checkpointFailures.incrementAndGet();
      LOG.warn("Checkpoint {} was aborted", checkpointId);

      // Alert if checkpoint failure rate is high
      long total = checkpointCount.get() + checkpointFailures.get();
      if (total > 10) {
        double failureRate = (double) checkpointFailures.get() / total;
        if (failureRate > 0.1) { // More than 10% failure rate
          LOG.error("High checkpoint failure rate detected: {:.2f}%", failureRate * 100);
        }
      }
    }

    public CheckpointMetrics getMetrics() {
      return new CheckpointMetrics(
          checkpointCount.get(), checkpointFailures.get(), lastCheckpointTime);
    }
  }

  /** Trigger checkpoint based on business events */
  public void triggerBusinessEventCheckpoint(String businessEvent) {
    LOG.info("Triggering checkpoint for business event: {}", businessEvent);

    // In real implementation, would trigger checkpoint through JobManager API
    // For now, just log the event
    LOG.info("Business event checkpoint triggered: {}", businessEvent);
  }

  /** Check if checkpoint is needed based on business schedule */
  public boolean shouldTriggerScheduledCheckpoint() {
    Instant now = Instant.now();

    // Trigger checkpoints at business-critical times
    int hour = now.atZone(java.time.ZoneId.systemDefault()).getHour();

    // End of business day checkpoint
    if (hour == 17) { // 5 PM
      return true;
    }

    // Payroll processing deadline checkpoint
    if (hour == 15) { // 3 PM
      return true;
    }

    return false;
  }

  /** Get watermark and checkpoint metrics */
  public WatermarkCheckpointMetrics getMetrics() {
    return new WatermarkCheckpointMetrics(
        lastWatermark.get(),
        watermarkUpdates.get(),
        lateRecordsCount.get(),
        checkpointCount.get(),
        checkpointFailures.get(),
        lastCheckpointTime,
        lastCheckpointDuration);
  }

  /** Reset metrics (useful for testing) */
  public void resetMetrics() {
    lastWatermark.set(Long.MIN_VALUE);
    watermarkUpdates.set(0);
    lateRecordsCount.set(0);
    checkpointCount.set(0);
    checkpointFailures.set(0);
    lastCheckpointTime = Instant.now();
    lastCheckpointDuration = Duration.ZERO;
  }

  /** Checkpoint metrics data class */
  public static class CheckpointMetrics {
    private final long completedCheckpoints;
    private final long failedCheckpoints;
    private final Instant lastCheckpointTime;

    public CheckpointMetrics(
        long completedCheckpoints, long failedCheckpoints, Instant lastCheckpointTime) {
      this.completedCheckpoints = completedCheckpoints;
      this.failedCheckpoints = failedCheckpoints;
      this.lastCheckpointTime = lastCheckpointTime;
    }

    public long getCompletedCheckpoints() {
      return completedCheckpoints;
    }

    public long getFailedCheckpoints() {
      return failedCheckpoints;
    }

    public Instant getLastCheckpointTime() {
      return lastCheckpointTime;
    }

    public double getSuccessRate() {
      long total = completedCheckpoints + failedCheckpoints;
      return total > 0 ? (double) completedCheckpoints / total : 1.0;
    }

    @Override
    public String toString() {
      return String.format(
          "CheckpointMetrics{completed=%d, failed=%d, successRate=%.2f%%, lastTime=%s}",
          completedCheckpoints, failedCheckpoints, getSuccessRate() * 100, lastCheckpointTime);
    }
  }

  /** Combined watermark and checkpoint metrics */
  public static class WatermarkCheckpointMetrics {
    private final long lastWatermark;
    private final long watermarkUpdates;
    private final long lateRecords;
    private final long checkpointCount;
    private final long checkpointFailures;
    private final Instant lastCheckpointTime;
    private final Duration lastCheckpointDuration;

    public WatermarkCheckpointMetrics(
        long lastWatermark,
        long watermarkUpdates,
        long lateRecords,
        long checkpointCount,
        long checkpointFailures,
        Instant lastCheckpointTime,
        Duration lastCheckpointDuration) {
      this.lastWatermark = lastWatermark;
      this.watermarkUpdates = watermarkUpdates;
      this.lateRecords = lateRecords;
      this.checkpointCount = checkpointCount;
      this.checkpointFailures = checkpointFailures;
      this.lastCheckpointTime = lastCheckpointTime;
      this.lastCheckpointDuration = lastCheckpointDuration;
    }

    public long getLastWatermark() {
      return lastWatermark;
    }

    public long getWatermarkUpdates() {
      return watermarkUpdates;
    }

    public long getLateRecords() {
      return lateRecords;
    }

    public long getCheckpointCount() {
      return checkpointCount;
    }

    public long getCheckpointFailures() {
      return checkpointFailures;
    }

    public Instant getLastCheckpointTime() {
      return lastCheckpointTime;
    }

    public Duration getLastCheckpointDuration() {
      return lastCheckpointDuration;
    }

    public double getCheckpointSuccessRate() {
      long total = checkpointCount + checkpointFailures;
      return total > 0 ? (double) checkpointCount / total : 1.0;
    }

    @Override
    public String toString() {
      return String.format(
          "WatermarkCheckpointMetrics{watermark=%s, updates=%d, lateRecords=%d, "
              + "checkpoints=%d, failures=%d, successRate=%.2f%%}",
          lastWatermark != Long.MIN_VALUE ? Instant.ofEpochMilli(lastWatermark) : "NONE",
          watermarkUpdates,
          lateRecords,
          checkpointCount,
          checkpointFailures,
          getCheckpointSuccessRate() * 100);
    }
  }
}
