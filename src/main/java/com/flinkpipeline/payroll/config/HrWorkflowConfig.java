package com.flinkpipeline.payroll.config;

import com.flinkpipeline.payroll.operators.HRWorkflowRoutingOperator;
import java.time.Duration;
import java.util.Map;

/**
 * Configuration for HR workflow routing and management.
 */
public class HrWorkflowConfig {
  private final Duration slaThreshold;
  private final int maxRetryAttempts;
  private final boolean loadBalancingEnabled;
  private final boolean escalationEnabled;
  private final Duration escalationThreshold;
  private final Map<String, HRWorkflowRoutingOperator.HRTeamConfig> hrTeamConfig;

  public HrWorkflowConfig(Duration slaThreshold, int maxRetryAttempts, boolean loadBalancingEnabled,
                         boolean escalationEnabled, Duration escalationThreshold,
                         Map<String, HRWorkflowRoutingOperator.HRTeamConfig> hrTeamConfig) {
    this.slaThreshold = slaThreshold;
    this.maxRetryAttempts = maxRetryAttempts;
    this.loadBalancingEnabled = loadBalancingEnabled;
    this.escalationEnabled = escalationEnabled;
    this.escalationThreshold = escalationThreshold;
    this.hrTeamConfig = hrTeamConfig;
  }

  public Duration getSlaThreshold() { return slaThreshold; }
  public int getMaxRetryAttempts() { return maxRetryAttempts; }
  public boolean isLoadBalancingEnabled() { return loadBalancingEnabled; }
  public boolean isEscalationEnabled() { return escalationEnabled; }
  public Duration getEscalationThreshold() { return escalationThreshold; }
  public Map<String, HRWorkflowRoutingOperator.HRTeamConfig> getHrTeamConfig() { return hrTeamConfig; }
}