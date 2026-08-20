package com.flinkpipeline.payroll.config;

import java.time.Duration;

/** Configuration for health check endpoints and monitoring. */
public class HealthCheckConfig {
  private final boolean enabled;
  private final int port;
  private final String endpoint;
  private final Duration checkInterval;

  public HealthCheckConfig(boolean enabled, int port, String endpoint, Duration checkInterval) {
    this.enabled = enabled;
    this.port = port;
    this.endpoint = endpoint;
    this.checkInterval = checkInterval;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getPort() {
    return port;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public Duration getCheckInterval() {
    return checkInterval;
  }
}
