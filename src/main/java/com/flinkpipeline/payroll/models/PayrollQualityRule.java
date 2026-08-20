package com.flinkpipeline.payroll.models;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Data model representing a configurable payroll data quality validation rule. Defines validation
 * logic, error handling, compliance requirements, and performance settings for payroll record
 * validation operations.
 */
public class PayrollQualityRule implements Serializable {

  private static final long serialVersionUID = 1L;

  public enum RuleType {
    FORMAT, // Field format validation (SSN, email patterns)
    RANGE, // Numeric/date range validation (age, wage limits)
    COMPLIANCE, // Regulatory compliance validation (PII, audit requirements)
    UNIQUENESS, // Duplicate detection and uniqueness constraints
    COMPLETENESS, // Required field presence and data completeness
    BUSINESS, // Business logic validation (employment eligibility)
    CROSS_FIELD // Multi-field validation rules
  }

  public enum ComplianceLevel {
    REGULATORY, // Federal/state regulatory requirements (must pass)
    BUSINESS, // Business policy requirements (should pass)
    INFORMATIONAL // Data quality information (may warn)
  }

  public enum ValidationMode {
    STRICT, // Fail immediately on rule violation
    WARNING, // Generate warning but allow processing
    CONDITIONAL // Apply rule based on conditions
  }

  // Core rule identification
  private final String ruleId;
  private final String ruleName;
  private final String description;
  private final String version;

  // Rule configuration
  private final String fieldName;
  private final RuleType ruleType;
  private final ComplianceLevel complianceLevel;
  private final ValidationMode validationMode;

  // Rule logic
  private final String validationExpression;
  private final String errorTemplate;
  private final String correctionGuidance;
  private final String businessJustification;

  // Performance settings
  private final boolean enabled;
  private final int priority;
  private final long cacheDurationMs;
  private final long timeoutMs;

  // Metadata
  private final Instant createdTimestamp;
  private final Instant lastModifiedTimestamp;
  private final String createdBy;
  private final String lastModifiedBy;

  // Constructor
  private PayrollQualityRule(Builder builder) {
    this.ruleId = builder.ruleId;
    this.ruleName = builder.ruleName;
    this.description = builder.description;
    this.version = builder.version;
    this.fieldName = builder.fieldName;
    this.ruleType = builder.ruleType;
    this.complianceLevel = builder.complianceLevel;
    this.validationMode = builder.validationMode;
    this.validationExpression = builder.validationExpression;
    this.errorTemplate = builder.errorTemplate;
    this.correctionGuidance = builder.correctionGuidance;
    this.businessJustification = builder.businessJustification;
    this.enabled = builder.enabled;
    this.priority = builder.priority;
    this.cacheDurationMs = builder.cacheDurationMs;
    this.timeoutMs = builder.timeoutMs;
    this.createdTimestamp = builder.createdTimestamp;
    this.lastModifiedTimestamp = builder.lastModifiedTimestamp;
    this.createdBy = builder.createdBy;
    this.lastModifiedBy = builder.lastModifiedBy;
  }

  // Static factory methods for common rules
  public static PayrollQualityRule createSSNValidationRule() {
    return new Builder()
        .ruleId("DQ-005")
        .ruleName("SSN Format Validation")
        .description("Validates Social Security Number format XXX-XX-XXXX")
        .fieldName("ssn")
        .ruleType(RuleType.FORMAT)
        .complianceLevel(ComplianceLevel.REGULATORY)
        .validationMode(ValidationMode.STRICT)
        .validationExpression("^\\d{3}-\\d{2}-\\d{4}$")
        .errorTemplate("SSN must be in format XXX-XX-XXXX")
        .correctionGuidance("Format SSN as 3 digits, hyphen, 2 digits, hyphen, 4 digits")
        .businessJustification("Federal tax reporting requires valid SSN format")
        .priority(10)
        .build();
  }

  public static PayrollQualityRule createAgeRangeValidationRule() {
    return new Builder()
        .ruleId("DQ-006")
        .ruleName("Employment Age Range Validation")
        .description("Validates employee age is within employment eligibility range")
        .fieldName("age")
        .ruleType(RuleType.RANGE)
        .complianceLevel(ComplianceLevel.REGULATORY)
        .validationMode(ValidationMode.STRICT)
        .validationExpression("age >= 16 && age <= 75")
        .errorTemplate("Employee age must be between 16 and 75 for employment eligibility")
        .correctionGuidance("Verify employee birthdate meets minimum age 16, maximum age 75")
        .businessJustification("Federal labor law compliance for employment eligibility")
        .priority(9)
        .build();
  }

  public static PayrollQualityRule createWageValidationRule() {
    return new Builder()
        .ruleId("DQ-007")
        .ruleName("Hourly Wage Compliance Validation")
        .description("Validates hourly wage meets federal minimum and maximum limits")
        .fieldName("hourly_rate_cents")
        .ruleType(RuleType.RANGE)
        .complianceLevel(ComplianceLevel.REGULATORY)
        .validationMode(ValidationMode.STRICT)
        .validationExpression("hourly_rate_cents >= 725 && hourly_rate_cents <= 15000")
        .errorTemplate("Hourly rate must be between $7.25 and $150.00")
        .correctionGuidance("Verify wage meets federal minimum $7.25/hour, maximum $150.00/hour")
        .businessJustification("Federal minimum wage and reasonable maximum wage policy")
        .priority(8)
        .build();
  }

  public static PayrollQualityRule createDuplicateDetectionRule() {
    return new Builder()
        .ruleId("DQ-008")
        .ruleName("Employee Duplicate Detection")
        .description("Detects duplicate employees based on SSN and email")
        .fieldName("employee_id")
        .ruleType(RuleType.UNIQUENESS)
        .complianceLevel(ComplianceLevel.BUSINESS)
        .validationMode(ValidationMode.WARNING)
        .validationExpression("DUPLICATE_CHECK(ssn, email)")
        .errorTemplate("Potential duplicate employee detected")
        .correctionGuidance("Review employee records for duplicates based on SSN or email")
        .businessJustification("Prevent duplicate payroll processing and maintain data integrity")
        .priority(7)
        .cacheDurationMs(3600000) // 1 hour cache for duplicate detection
        .build();
  }

  public static PayrollQualityRule createEmailValidationRule() {
    return new Builder()
        .ruleId("DQ-001")
        .ruleName("Email Format Validation")
        .description("Validates employee email format")
        .fieldName("email")
        .ruleType(RuleType.FORMAT)
        .complianceLevel(ComplianceLevel.BUSINESS)
        .validationMode(ValidationMode.STRICT)
        .validationExpression("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
        .errorTemplate("Email must be in valid format")
        .correctionGuidance("Enter a valid email address")
        .businessJustification("Valid email required for employee communication")
        .priority(5)
        .build();
  }

  public static PayrollQualityRule createAgeRangeRule() {
    return createAgeRangeValidationRule();
  }

  public static PayrollQualityRule createWageComplianceRule() {
    return createWageValidationRule();
  }

  // Getters
  public String getRuleId() {
    return ruleId;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getDescription() {
    return description;
  }

  public String getVersion() {
    return version;
  }

  public String getFieldName() {
    return fieldName;
  }

  public RuleType getRuleType() {
    return ruleType;
  }

  public ComplianceLevel getComplianceLevel() {
    return complianceLevel;
  }

  public ValidationMode getValidationMode() {
    return validationMode;
  }

  public String getValidationExpression() {
    return validationExpression;
  }

  public String getErrorTemplate() {
    return errorTemplate;
  }

  public String getCorrectionGuidance() {
    return correctionGuidance;
  }

  public String getBusinessJustification() {
    return businessJustification;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getPriority() {
    return priority;
  }

  public long getCacheDurationMs() {
    return cacheDurationMs;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public Instant getCreatedTimestamp() {
    return createdTimestamp;
  }

  public Instant getLastModifiedTimestamp() {
    return lastModifiedTimestamp;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getLastModifiedBy() {
    return lastModifiedBy;
  }

  // Business logic methods
  public boolean isRegulatory() {
    return complianceLevel == ComplianceLevel.REGULATORY;
  }

  public boolean isStrict() {
    return validationMode == ValidationMode.STRICT;
  }

  public boolean requiresCache() {
    return cacheDurationMs > 0;
  }

  public boolean hasTimeout() {
    return timeoutMs > 0;
  }

  public String formatErrorMessage(String fieldValue) {
    if (errorTemplate == null) return "Validation failed for field: " + fieldName;
    return errorTemplate
        .replace("{field_value}", fieldValue != null ? fieldValue : "null")
        .replace("{field_name}", fieldName);
  }

  // Builder pattern
  public static class Builder {
    private String ruleId;
    private String ruleName;
    private String description;
    private String version = "1.0.0";
    private String fieldName;
    private RuleType ruleType;
    private ComplianceLevel complianceLevel = ComplianceLevel.BUSINESS;
    private ValidationMode validationMode = ValidationMode.STRICT;
    private String validationExpression;
    private String errorTemplate;
    private String correctionGuidance;
    private String businessJustification;
    private boolean enabled = true;
    private int priority = 5;
    private long cacheDurationMs = 0;
    private long timeoutMs = 5000; // 5 second default timeout
    private Instant createdTimestamp = Instant.now();
    private Instant lastModifiedTimestamp = Instant.now();
    private String createdBy = "SYSTEM";
    private String lastModifiedBy = "SYSTEM";

    public Builder ruleId(String ruleId) {
      this.ruleId = ruleId;
      return this;
    }

    public Builder ruleName(String ruleName) {
      this.ruleName = ruleName;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder version(String version) {
      this.version = version;
      return this;
    }

    public Builder fieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder ruleType(RuleType ruleType) {
      this.ruleType = ruleType;
      return this;
    }

    public Builder complianceLevel(ComplianceLevel complianceLevel) {
      this.complianceLevel = complianceLevel;
      return this;
    }

    public Builder validationMode(ValidationMode validationMode) {
      this.validationMode = validationMode;
      return this;
    }

    public Builder validationExpression(String validationExpression) {
      this.validationExpression = validationExpression;
      return this;
    }

    public Builder errorTemplate(String errorTemplate) {
      this.errorTemplate = errorTemplate;
      return this;
    }

    public Builder correctionGuidance(String correctionGuidance) {
      this.correctionGuidance = correctionGuidance;
      return this;
    }

    public Builder businessJustification(String businessJustification) {
      this.businessJustification = businessJustification;
      return this;
    }

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder priority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder cacheDurationMs(long cacheDurationMs) {
      this.cacheDurationMs = cacheDurationMs;
      return this;
    }

    public Builder timeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public Builder createdTimestamp(Instant createdTimestamp) {
      this.createdTimestamp = createdTimestamp;
      return this;
    }

    public Builder lastModifiedTimestamp(Instant lastModifiedTimestamp) {
      this.lastModifiedTimestamp = lastModifiedTimestamp;
      return this;
    }

    public Builder createdBy(String createdBy) {
      this.createdBy = createdBy;
      return this;
    }

    public Builder lastModifiedBy(String lastModifiedBy) {
      this.lastModifiedBy = lastModifiedBy;
      return this;
    }

    public PayrollQualityRule build() {
      // Validation
      Objects.requireNonNull(ruleId, "Rule ID is required");
      Objects.requireNonNull(ruleName, "Rule name is required");
      Objects.requireNonNull(fieldName, "Field name is required");
      Objects.requireNonNull(ruleType, "Rule type is required");
      Objects.requireNonNull(validationExpression, "Validation expression is required");

      return new PayrollQualityRule(this);
    }
  }

  // Object methods
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    PayrollQualityRule that = (PayrollQualityRule) obj;
    return Objects.equals(ruleId, that.ruleId) && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ruleId, version);
  }

  @Override
  public String toString() {
    return "PayrollQualityRule{"
        + "ruleId='"
        + ruleId
        + '\''
        + ", ruleName='"
        + ruleName
        + '\''
        + ", fieldName='"
        + fieldName
        + '\''
        + ", ruleType="
        + ruleType
        + ", complianceLevel="
        + complianceLevel
        + ", enabled="
        + enabled
        + ", priority="
        + priority
        + '}';
  }
}
