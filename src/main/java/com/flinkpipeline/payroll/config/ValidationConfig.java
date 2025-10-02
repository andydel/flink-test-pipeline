package com.flinkpipeline.payroll.config;

import com.flinkpipeline.payroll.models.PayrollQualityRule;
import java.time.Duration;
import java.util.List;

/**
 * Configuration for validation operators and rules.
 */
public class ValidationConfig {
  private final List<PayrollQualityRule> qualityRules;
  private final boolean duplicateDetectionEnabled;
  private final Duration duplicateDetectionWindow;
  private final boolean complianceAuditingEnabled;
  private final boolean strictModeEnabled;

  public ValidationConfig(List<PayrollQualityRule> qualityRules, boolean duplicateDetectionEnabled,
                         Duration duplicateDetectionWindow, boolean complianceAuditingEnabled,
                         boolean strictModeEnabled) {
    this.qualityRules = qualityRules;
    this.duplicateDetectionEnabled = duplicateDetectionEnabled;
    this.duplicateDetectionWindow = duplicateDetectionWindow;
    this.complianceAuditingEnabled = complianceAuditingEnabled;
    this.strictModeEnabled = strictModeEnabled;
  }

  public List<PayrollQualityRule> getQualityRules() { return qualityRules; }
  public boolean isDuplicateDetectionEnabled() { return duplicateDetectionEnabled; }
  public Duration getDuplicateDetectionWindow() { return duplicateDetectionWindow; }
  public boolean isComplianceAuditingEnabled() { return complianceAuditingEnabled; }
  public boolean isStrictModeEnabled() { return strictModeEnabled; }
}