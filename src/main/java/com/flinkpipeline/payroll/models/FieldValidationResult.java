package com.flinkpipeline.payroll.models;

import java.util.Objects;

/**
 * Represents the validation outcome for an individual payroll field. Contains detailed information
 * about field-specific validation results.
 */
public class FieldValidationResult {

  private String fieldName;
  private String ruleName;
  private RuleType ruleType;
  private FieldStatus status;
  private String errorMessage;
  private Severity severity;
  private String suggestedCorrection;
  private String errorCode;
  private ComplianceLevel complianceLevel;

  // Default constructor
  public FieldValidationResult() {}

  // Constructor with basic fields
  public FieldValidationResult(String fieldName, String ruleName, FieldStatus status) {
    this.fieldName = fieldName;
    this.ruleName = ruleName;
    this.status = status;
  }

  // Getters and Setters

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getRuleName() {
    return ruleName;
  }

  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public RuleType getRuleType() {
    return ruleType;
  }

  public void setRuleType(RuleType ruleType) {
    this.ruleType = ruleType;
  }

  public FieldStatus getStatus() {
    return status;
  }

  public void setStatus(FieldStatus status) {
    this.status = status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public Severity getSeverity() {
    return severity;
  }

  public void setSeverity(Severity severity) {
    this.severity = severity;
  }

  public String getSuggestedCorrection() {
    return suggestedCorrection;
  }

  public void setSuggestedCorrection(String suggestedCorrection) {
    this.suggestedCorrection = suggestedCorrection;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public ComplianceLevel getComplianceLevel() {
    return complianceLevel;
  }

  public void setComplianceLevel(ComplianceLevel complianceLevel) {
    this.complianceLevel = complianceLevel;
  }

  // Utility methods

  /** Checks if this field validation passed */
  public boolean isPassed() {
    return status == FieldStatus.PASSED;
  }

  /** Checks if this field validation failed */
  public boolean isFailed() {
    return status == FieldStatus.FAILED;
  }

  /** Checks if this is a warning */
  public boolean isWarning() {
    return status == FieldStatus.WARNING;
  }

  /** Checks if this is a regulatory compliance issue */
  public boolean isRegulatoryViolation() {
    return complianceLevel == ComplianceLevel.REGULATORY && isFailed();
  }

  /** Creates a successful validation result */
  public static FieldValidationResult success(String fieldName, String ruleName) {
    FieldValidationResult result =
        new FieldValidationResult(fieldName, ruleName, FieldStatus.PASSED);
    result.setSeverity(Severity.INFO);
    return result;
  }

  /** Creates a failed validation result with error details */
  public static FieldValidationResult failure(
      String fieldName, String ruleName, String errorMessage, String suggestedCorrection) {
    FieldValidationResult result =
        new FieldValidationResult(fieldName, ruleName, FieldStatus.FAILED);
    result.setErrorMessage(errorMessage);
    result.setSuggestedCorrection(suggestedCorrection);
    result.setSeverity(Severity.CRITICAL);
    return result;
  }

  /** Creates a failed validation result with error details and compliance level */
  public static FieldValidationResult failure(
      String fieldName,
      String ruleName,
      String errorMessage,
      String suggestedCorrection,
      ComplianceLevel complianceLevel) {
    FieldValidationResult result =
        new FieldValidationResult(fieldName, ruleName, FieldStatus.FAILED);
    result.setErrorMessage(errorMessage);
    result.setSuggestedCorrection(suggestedCorrection);
    result.setSeverity(Severity.CRITICAL);
    result.setComplianceLevel(complianceLevel);
    return result;
  }

  /** Creates a warning validation result */
  public static FieldValidationResult warning(String fieldName, String ruleName, String message) {
    FieldValidationResult result =
        new FieldValidationResult(fieldName, ruleName, FieldStatus.WARNING);
    result.setErrorMessage(message);
    result.setSeverity(Severity.WARNING);
    return result;
  }

  /** Creates a warning validation result with additional parameters */
  public static FieldValidationResult warning(
      String fieldName,
      String ruleName,
      String message,
      String suggestedCorrection,
      ComplianceLevel complianceLevel) {
    FieldValidationResult result =
        new FieldValidationResult(fieldName, ruleName, FieldStatus.WARNING);
    result.setErrorMessage(message);
    result.setSuggestedCorrection(suggestedCorrection);
    result.setSeverity(Severity.WARNING);
    result.setComplianceLevel(complianceLevel);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FieldValidationResult that = (FieldValidationResult) obj;
    return Objects.equals(fieldName, that.fieldName)
        && Objects.equals(ruleName, that.ruleName)
        && ruleType == that.ruleType
        && status == that.status
        && Objects.equals(errorMessage, that.errorMessage)
        && severity == that.severity
        && Objects.equals(suggestedCorrection, that.suggestedCorrection)
        && Objects.equals(errorCode, that.errorCode)
        && complianceLevel == that.complianceLevel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldName,
        ruleName,
        ruleType,
        status,
        errorMessage,
        severity,
        suggestedCorrection,
        errorCode,
        complianceLevel);
  }

  @Override
  public String toString() {
    return "FieldValidationResult{"
        + "fieldName='"
        + fieldName
        + '\''
        + ", ruleName='"
        + ruleName
        + '\''
        + ", ruleType="
        + ruleType
        + ", status="
        + status
        + ", errorMessage='"
        + errorMessage
        + '\''
        + ", severity="
        + severity
        + ", suggestedCorrection='"
        + suggestedCorrection
        + '\''
        + ", errorCode='"
        + errorCode
        + '\''
        + ", complianceLevel="
        + complianceLevel
        + '}';
  }

  /** Rule type enumeration */
  public enum RuleType {
    FORMAT, // Pattern/format validation (SSN, email)
    RANGE, // Numeric/date range validation (age, hourly rate)
    COMPLIANCE, // Regulatory/compliance validation
    UNIQUENESS, // Duplicate detection
    COMPLETENESS // Required field validation
  }

  /** Field validation status enumeration */
  public enum FieldStatus {
    PASSED,
    FAILED,
    WARNING
  }

  /** Severity enumeration */
  public enum Severity {
    CRITICAL, // Blocks processing, requires immediate attention
    HIGH, // Important issue, should be resolved soon
    MEDIUM, // Moderate issue, can be addressed later
    LOW, // Minor issue, informational
    WARNING, // Warning, doesn't block processing
    INFO // Informational, no action required
  }

  /** Compliance level enumeration */
  public enum ComplianceLevel {
    REGULATORY, // Violates regulatory requirements
    BUSINESS, // Violates business rules
    INFORMATIONAL // Informational only
  }

  /** Builder pattern for creating FieldValidationResult instances */
  public static class Builder {
    private String fieldName;
    private String ruleName;
    private RuleType ruleType;
    private FieldStatus status;
    private String errorMessage;
    private Severity severity;
    private String suggestedCorrection;
    private String errorCode;
    private ComplianceLevel complianceLevel;

    public Builder fieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder ruleName(String ruleName) {
      this.ruleName = ruleName;
      return this;
    }

    public Builder ruleType(RuleType ruleType) {
      this.ruleType = ruleType;
      return this;
    }

    public Builder status(FieldStatus status) {
      this.status = status;
      return this;
    }

    public Builder errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public Builder severity(Severity severity) {
      this.severity = severity;
      return this;
    }

    public Builder suggestedCorrection(String suggestedCorrection) {
      this.suggestedCorrection = suggestedCorrection;
      return this;
    }

    public Builder errorCode(String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    public Builder complianceLevel(ComplianceLevel complianceLevel) {
      this.complianceLevel = complianceLevel;
      return this;
    }

    public FieldValidationResult build() {
      FieldValidationResult result = new FieldValidationResult();
      result.setFieldName(fieldName);
      result.setRuleName(ruleName);
      result.setRuleType(ruleType);
      result.setStatus(status);
      result.setErrorMessage(errorMessage);
      result.setSeverity(severity);
      result.setSuggestedCorrection(suggestedCorrection);
      result.setErrorCode(errorCode);
      result.setComplianceLevel(complianceLevel);
      return result;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
