package com.flinkpipeline.payroll.config;

import java.time.Duration;

/**
 * Configuration for Flink execution environment settings.
 */
public class ExecutionConfig {
  private final int parallelism;
  private final int maxParallelism;
  private final Duration bufferTimeout;
  private final String timeCharacteristic;

  public ExecutionConfig(int parallelism, int maxParallelism, Duration bufferTimeout, String timeCharacteristic) {
    this.parallelism = parallelism;
    this.maxParallelism = maxParallelism;
    this.bufferTimeout = bufferTimeout;
    this.timeCharacteristic = timeCharacteristic;
  }

  public int getParallelism() { return parallelism; }
  public int getMaxParallelism() { return maxParallelism; }
  public Duration getBufferTimeout() { return bufferTimeout; }
  public String getTimeCharacteristic() { return timeCharacteristic; }
}