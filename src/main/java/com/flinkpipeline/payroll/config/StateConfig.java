package com.flinkpipeline.payroll.config;

/** Configuration for Flink state management and storage. */
public class StateConfig {
  private final String backend;
  private final String storagePath;
  private final boolean incrementalCheckpoints;
  private final long ttlMilliseconds;

  public StateConfig(
      String backend, String storagePath, boolean incrementalCheckpoints, long ttlMilliseconds) {
    this.backend = backend;
    this.storagePath = storagePath;
    this.incrementalCheckpoints = incrementalCheckpoints;
    this.ttlMilliseconds = ttlMilliseconds;
  }

  public String getBackend() {
    return backend;
  }

  public String getStoragePath() {
    return storagePath;
  }

  public boolean isIncrementalCheckpoints() {
    return incrementalCheckpoints;
  }

  public long getTtlMilliseconds() {
    return ttlMilliseconds;
  }
}
