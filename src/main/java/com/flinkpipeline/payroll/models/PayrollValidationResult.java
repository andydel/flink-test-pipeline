package com.flinkpipeline.payroll.models;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents the detailed outcome of payroll record validation.
 * Contains validation status, field-level results, and processing metadata.
 */
public class PayrollValidationResult {

  private Integer employeeId;
  private Long validationTimestamp;
  private ValidationStatus overallStatus;
  private List<FieldValidationResult> fieldResults;
  private List<ComplianceFlag> complianceFlags;
  private Long processingLatencyMs;
  private String ruleVersion;

  // Default constructor
  public PayrollValidationResult() {
    this.fieldResults = new ArrayList<>();
    this.complianceFlags = new ArrayList<>();
    this.validationTimestamp = Instant.now().toEpochMilli();
  }

  // Constructor with employee ID
  public PayrollValidationResult(Integer employeeId) {
    this();
    this.employeeId = employeeId;
  }

  // Getters and Setters

  public Integer getEmployeeId() {
    return employeeId;
  }

  public void setEmployeeId(Integer employeeId) {
    this.employeeId = employeeId;
  }

  public Long getValidationTimestamp() {
    return validationTimestamp;
  }

  public void setValidationTimestamp(Long validationTimestamp) {
    this.validationTimestamp = validationTimestamp;
  }

  public ValidationStatus getOverallStatus() {
    return overallStatus;
  }

  public void setOverallStatus(ValidationStatus overallStatus) {
    this.overallStatus = overallStatus;
  }

  public List<FieldValidationResult> getFieldResults() {
    return fieldResults;
  }

  public void setFieldResults(List<FieldValidationResult> fieldResults) {
    this.fieldResults = fieldResults != null ? fieldResults : new ArrayList<>();
  }

  public List<ComplianceFlag> getComplianceFlags() {
    return complianceFlags;
  }

  public void setComplianceFlags(List<ComplianceFlag> complianceFlags) {
    this.complianceFlags = complianceFlags != null ? complianceFlags : new ArrayList<>();
  }

  public Long getProcessingLatencyMs() {
    return processingLatencyMs;
  }

  public void setProcessingLatencyMs(Long processingLatencyMs) {
    this.processingLatencyMs = processingLatencyMs;
  }

  public String getRuleVersion() {
    return ruleVersion;
  }

  public void setRuleVersion(String ruleVersion) {
    this.ruleVersion = ruleVersion;
  }

  // Utility methods

  /**
   * Adds a field validation result
   */
  public void addFieldResult(FieldValidationResult fieldResult) {
    if (this.fieldResults == null) {
      this.fieldResults = new ArrayList<>();
    }
    this.fieldResults.add(fieldResult);
  }

  /**
   * Adds a compliance flag
   */
  public void addComplianceFlag(ComplianceFlag flag) {
    if (this.complianceFlags == null) {
      this.complianceFlags = new ArrayList<>();
    }
    this.complianceFlags.add(flag);
  }

  /**
   * Checks if validation was successful (no failures)
   */
  public boolean isValid() {
    return overallStatus == ValidationStatus.VALID;
  }

  /**
   * Checks if there are any validation errors
   */
  public boolean hasErrors() {
    return fieldResults != null && fieldResults.stream()
        .anyMatch(result -> result.getStatus() == FieldStatus.FAILED);
  }

  /**
   * Checks if there are any compliance violations
   */
  public boolean hasComplianceViolations() {
    return overallStatus == ValidationStatus.COMPLIANCE_VIOLATION ||
           (complianceFlags != null && !complianceFlags.isEmpty());
  }

  /**
   * Gets all error messages from failed field validations
   */
  public List<String> getErrorMessages() {
    List<String> errors = new ArrayList<>();
    if (fieldResults != null) {
      fieldResults.stream()
          .filter(result -> result.getStatus() == FieldStatus.FAILED)
          .forEach(result -> errors.add(result.getErrorMessage()));
    }
    return errors;
  }

  /**
   * Gets count of failed validations
   */
  public int getFailureCount() {
    if (fieldResults == null) return 0;
    return (int) fieldResults.stream()
        .filter(result -> result.getStatus() == FieldStatus.FAILED)
        .count();
  }

  /**
   * Gets count of successful validations
   */
  public int getSuccessCount() {
    if (fieldResults == null) return 0;
    return (int) fieldResults.stream()
        .filter(result -> result.getStatus() == FieldStatus.PASSED)
        .count();
  }

  /**
   * Checks if processing met the 50ms SLA requirement
   */
  public boolean meetsLatencySLA() {
    return processingLatencyMs != null && processingLatencyMs <= 50;
  }

  /**
   * Updates overall status based on field results
   */
  public void updateOverallStatus() {
    if (hasErrors()) {
      if (hasComplianceViolations()) {
        this.overallStatus = ValidationStatus.COMPLIANCE_VIOLATION;
      } else {
        this.overallStatus = ValidationStatus.INVALID;
      }
    } else {
      this.overallStatus = ValidationStatus.VALID;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    PayrollValidationResult that = (PayrollValidationResult) obj;
    return Objects.equals(employeeId, that.employeeId) &&
           Objects.equals(validationTimestamp, that.validationTimestamp) &&
           overallStatus == that.overallStatus &&
           Objects.equals(fieldResults, that.fieldResults) &&
           Objects.equals(complianceFlags, that.complianceFlags) &&
           Objects.equals(processingLatencyMs, that.processingLatencyMs) &&
           Objects.equals(ruleVersion, that.ruleVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(employeeId, validationTimestamp, overallStatus,
        fieldResults, complianceFlags, processingLatencyMs, ruleVersion);
  }

  @Override
  public String toString() {
    return "PayrollValidationResult{" +
           "employeeId=" + employeeId +
           ", validationTimestamp=" + validationTimestamp +
           ", overallStatus=" + overallStatus +
           ", fieldResults=" + fieldResults +
           ", complianceFlags=" + complianceFlags +
           ", processingLatencyMs=" + processingLatencyMs +
           ", ruleVersion='" + ruleVersion + '\'' +
           '}';
  }

  /**
   * Validation status enumeration
   */
  public enum ValidationStatus {
    VALID,
    INVALID,
    COMPLIANCE_VIOLATION
  }

  /**
   * Compliance flag enumeration
   */
  public enum ComplianceFlag {
    PII_ENCRYPTION_REQUIRED,
    REGULATORY_VIOLATION,
    AUDIT_REQUIRED,
    REVIEW_REQUIRED
  }

  /**
   * Field validation status enumeration
   */
  public enum FieldStatus {
    PASSED,
    FAILED,
    WARNING
  }

  /**
   * Builder pattern for creating PayrollValidationResult instances
   */
  public static class Builder {
    private Integer employeeId;
    private ValidationStatus overallStatus;
    private List<FieldValidationResult> fieldResults = new ArrayList<>();
    private List<ComplianceFlag> complianceFlags = new ArrayList<>();
    private Long processingLatencyMs;
    private String ruleVersion;

    public Builder employeeId(Integer employeeId) {
      this.employeeId = employeeId;
      return this;
    }

    public Builder overallStatus(ValidationStatus status) {
      this.overallStatus = status;
      return this;
    }

    public Builder addFieldResult(FieldValidationResult result) {
      this.fieldResults.add(result);
      return this;
    }

    public Builder addComplianceFlag(ComplianceFlag flag) {
      this.complianceFlags.add(flag);
      return this;
    }

    public Builder processingLatencyMs(Long latency) {
      this.processingLatencyMs = latency;
      return this;
    }

    public Builder ruleVersion(String version) {
      this.ruleVersion = version;
      return this;
    }

    public PayrollValidationResult build() {
      PayrollValidationResult result = new PayrollValidationResult(employeeId);
      result.setOverallStatus(overallStatus);
      result.setFieldResults(fieldResults);
      result.setComplianceFlags(complianceFlags);
      result.setProcessingLatencyMs(processingLatencyMs);
      result.setRuleVersion(ruleVersion);
      return result;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}