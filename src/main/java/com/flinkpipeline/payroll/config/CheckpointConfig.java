package com.flinkpipeline.payroll.config;

/**
 * Configuration for Flink checkpointing and fault tolerance.
 */
public class CheckpointConfig {
  private final boolean enabled;
  private final long interval;
  private final long minPause;
  private final long timeout;
  private final String storagePath;

  public CheckpointConfig(boolean enabled, long interval, long minPause, long timeout, String storagePath) {
    this.enabled = enabled;
    this.interval = interval;
    this.minPause = minPause;
    this.timeout = timeout;
    this.storagePath = storagePath;
  }

  public boolean isEnabled() { return enabled; }
  public long getInterval() { return interval; }
  public long getMinPause() { return minPause; }
  public long getTimeout() { return timeout; }
  public String getStoragePath() { return storagePath; }
}