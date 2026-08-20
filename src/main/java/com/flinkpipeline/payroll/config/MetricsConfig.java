package com.flinkpipeline.payroll.config;

import java.time.Duration;

/** Configuration for metrics collection and reporting. */
public class MetricsConfig {
  private final boolean enabled;
  private final String reporter;
  private final Duration interval;
  private final String endpoint;

  public MetricsConfig(boolean enabled, String reporter, Duration interval, String endpoint) {
    this.enabled = enabled;
    this.reporter = reporter;
    this.interval = interval;
    this.endpoint = endpoint;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getReporter() {
    return reporter;
  }

  public Duration getInterval() {
    return interval;
  }

  public String getEndpoint() {
    return endpoint;
  }
}
