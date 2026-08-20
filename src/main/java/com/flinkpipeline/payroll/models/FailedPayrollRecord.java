package com.flinkpipeline.payroll.models;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data model representing a payroll record that failed validation with comprehensive error details.
 * Contains the original employee record, validation results, HR workflow information, and
 * correction guidance for payroll data quality management.
 */
public class FailedPayrollRecord {

  public enum Priority {
    CRITICAL, // Regulatory violations, blocks payroll processing
    HIGH, // Business rule violations, impacts operations
    MEDIUM, // Data quality issues, requires attention
    LOW // Informational issues, minor corrections needed
  }

  public enum RiskLevel {
    REGULATORY, // Compliance/regulatory risk requiring immediate attention
    FINANCIAL, // Financial impact risk (incorrect wages, tax issues)
    OPERATIONAL, // Operational risk affecting payroll processing
    DATA_QUALITY // Data quality risk with minimal business impact
  }

  public enum CorrectionStatus {
    PENDING, // Awaiting HR correction
    IN_PROGRESS, // HR is working on correction
    CORRECTED, // Correction submitted, awaiting validation
    RESOLVED, // Successfully corrected and validated
    ESCALATED, // Escalated due to multiple failed attempts
    PERMANENTLY_FAILED // Cannot be corrected, requires manual handling
  }

  // Core record information
  private final PayrollEmployee originalRecord;
  private final PayrollValidationResult validationResult;
  private final Instant failureTimestamp;
  private final long processingLatencyMs;

  // HR workflow information
  private final String hrWorkflowId;
  private final Priority correctionPriority;
  private final RiskLevel complianceRiskLevel;
  private final CorrectionStatus correctionStatus;
  private final Integer estimatedCorrectionTimeMinutes;

  // Error details and guidance
  private final List<String> validationErrors;
  private final List<String> complianceFlags;
  private final String hrCorrectionInstructions;
  private final String primaryFailureReason;
  private final String businessImpactDescription;

  // Correction tracking
  private final Integer correctionAttempts;
  private final Instant lastCorrectionAttempt;
  private final String assignedHRUser;
  private final Instant hrResponseDeadline;

  // Metadata
  private final String sourceSystem;
  private final String pipelineVersion;
  private final String correlationId;

  // Constructor
  private FailedPayrollRecord(Builder builder) {
    this.originalRecord = builder.originalRecord;
    this.validationResult = builder.validationResult;
    this.failureTimestamp = builder.failureTimestamp;
    this.processingLatencyMs = builder.processingLatencyMs;
    this.hrWorkflowId = builder.hrWorkflowId;
    this.correctionPriority = builder.correctionPriority;
    this.complianceRiskLevel = builder.complianceRiskLevel;
    this.correctionStatus = builder.correctionStatus;
    this.estimatedCorrectionTimeMinutes = builder.estimatedCorrectionTimeMinutes;
    this.validationErrors = new ArrayList<>(builder.validationErrors);
    this.complianceFlags = new ArrayList<>(builder.complianceFlags);
    this.hrCorrectionInstructions = builder.hrCorrectionInstructions;
    this.primaryFailureReason = builder.primaryFailureReason;
    this.businessImpactDescription = builder.businessImpactDescription;
    this.correctionAttempts = builder.correctionAttempts;
    this.lastCorrectionAttempt = builder.lastCorrectionAttempt;
    this.assignedHRUser = builder.assignedHRUser;
    this.hrResponseDeadline = builder.hrResponseDeadline;
    this.sourceSystem = builder.sourceSystem;
    this.pipelineVersion = builder.pipelineVersion;
    this.correlationId = builder.correlationId;
  }

  // Static factory method from validation result
  public static FailedPayrollRecord fromValidationResult(
      PayrollEmployee employee, PayrollValidationResult result) {
    Builder builder =
        new Builder()
            .originalRecord(employee)
            .validationResult(result)
            .failureTimestamp(Instant.now())
            .processingLatencyMs(result.getProcessingLatencyMs())
            .hrWorkflowId(generateHRWorkflowId(employee.getEmployeeId()))
            .correctionStatus(CorrectionStatus.PENDING)
            .correctionAttempts(0);

    // Extract validation errors
    List<String> errors = new ArrayList<>();
    List<String> flags = new ArrayList<>();
    for (FieldValidationResult fieldResult : result.getFieldResults()) {
      if (fieldResult.isFailed()) {
        errors.add(fieldResult.getErrorMessage());
        if (fieldResult.getComplianceLevel() == FieldValidationResult.ComplianceLevel.REGULATORY) {
          flags.add("REGULATORY_VIOLATION");
        }
      }
    }

    builder.validationErrors(errors).complianceFlags(flags);

    // Determine priority and risk level
    if (result.hasRegulatoryViolations()) {
      builder
          .correctionPriority(Priority.CRITICAL)
          .complianceRiskLevel(RiskLevel.REGULATORY)
          .estimatedCorrectionTimeMinutes(30);
    } else if (result.hasBusinessViolations()) {
      builder
          .correctionPriority(Priority.HIGH)
          .complianceRiskLevel(RiskLevel.OPERATIONAL)
          .estimatedCorrectionTimeMinutes(120);
    } else {
      builder
          .correctionPriority(Priority.MEDIUM)
          .complianceRiskLevel(RiskLevel.DATA_QUALITY)
          .estimatedCorrectionTimeMinutes(240);
    }

    // Generate HR correction instructions
    builder
        .hrCorrectionInstructions(generateCorrectionInstructions(result))
        .primaryFailureReason(determinePrimaryFailureReason(result))
        .businessImpactDescription(assessBusinessImpact(result));

    return builder.build();
  }

  // Getters
  public PayrollEmployee getOriginalRecord() {
    return originalRecord;
  }

  public PayrollValidationResult getValidationResult() {
    return validationResult;
  }

  public Instant getFailureTimestamp() {
    return failureTimestamp;
  }

  public long getProcessingLatencyMs() {
    return processingLatencyMs;
  }

  public String getHrWorkflowId() {
    return hrWorkflowId;
  }

  public Priority getCorrectionPriority() {
    return correctionPriority;
  }

  public RiskLevel getComplianceRiskLevel() {
    return complianceRiskLevel;
  }

  public CorrectionStatus getCorrectionStatus() {
    return correctionStatus;
  }

  public Integer getEstimatedCorrectionTimeMinutes() {
    return estimatedCorrectionTimeMinutes;
  }

  public List<String> getValidationErrors() {
    return new ArrayList<>(validationErrors);
  }

  public List<String> getComplianceFlags() {
    return new ArrayList<>(complianceFlags);
  }

  public String getHrCorrectionInstructions() {
    return hrCorrectionInstructions;
  }

  public String getPrimaryFailureReason() {
    return primaryFailureReason;
  }

  public String getBusinessImpactDescription() {
    return businessImpactDescription;
  }

  public Integer getCorrectionAttempts() {
    return correctionAttempts;
  }

  public Instant getLastCorrectionAttempt() {
    return lastCorrectionAttempt;
  }

  public String getAssignedHRUser() {
    return assignedHRUser;
  }

  public Instant getHrResponseDeadline() {
    return hrResponseDeadline;
  }

  public String getSourceSystem() {
    return sourceSystem;
  }

  public String getPipelineVersion() {
    return pipelineVersion;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  // Convenience getters
  public Integer getEmployeeId() {
    return originalRecord != null ? originalRecord.getEmployeeId() : null;
  }

  public String getEmployeeName() {
    if (originalRecord == null) return "Unknown";
    return originalRecord.getFirstName() + " " + originalRecord.getLastName();
  }

  public boolean isCritical() {
    return correctionPriority == Priority.CRITICAL;
  }

  public boolean hasRegulatoryRisk() {
    return complianceRiskLevel == RiskLevel.REGULATORY;
  }

  public boolean isOverdue() {
    return hrResponseDeadline != null && Instant.now().isAfter(hrResponseDeadline);
  }

  public boolean hasMultipleAttempts() {
    return correctionAttempts != null && correctionAttempts > 1;
  }

  // Business logic methods
  public FailedPayrollRecord withCorrectionAttempt(String hrUserId, String correctionNote) {
    return new Builder(this)
        .correctionStatus(CorrectionStatus.IN_PROGRESS)
        .correctionAttempts((correctionAttempts != null ? correctionAttempts : 0) + 1)
        .lastCorrectionAttempt(Instant.now())
        .assignedHRUser(hrUserId)
        .build();
  }

  public FailedPayrollRecord withResolution() {
    return new Builder(this).correctionStatus(CorrectionStatus.RESOLVED).build();
  }

  public FailedPayrollRecord withEscalation() {
    return new Builder(this)
        .correctionStatus(CorrectionStatus.ESCALATED)
        .correctionPriority(Priority.CRITICAL)
        .build();
  }

  // Helper methods for generation
  private static String generateHRWorkflowId(Integer employeeId) {
    return "HR-WF-" + employeeId + "-" + System.currentTimeMillis();
  }

  private static String generateCorrectionInstructions(PayrollValidationResult result) {
    StringBuilder instructions = new StringBuilder();
    instructions.append("PAYROLL RECORD CORRECTION REQUIRED\n\n");

    for (FieldValidationResult fieldResult : result.getFieldResults()) {
      if (fieldResult.isFailed()) {
        instructions
            .append("Field: ")
            .append(fieldResult.getFieldName().toUpperCase())
            .append("\n");
        instructions.append("Issue: ").append(fieldResult.getErrorMessage()).append("\n");
        instructions.append("Action: ").append(fieldResult.getSuggestedCorrection()).append("\n\n");
      }
    }

    instructions.append("Priority: ");
    if (result.hasRegulatoryViolations()) {
      instructions.append("CRITICAL - Regulatory compliance violation\n");
      instructions.append("Deadline: 30 minutes from notification\n");
    } else {
      instructions.append("HIGH - Business rule violation\n");
      instructions.append("Deadline: 4 hours from notification\n");
    }

    return instructions.toString();
  }

  private static String determinePrimaryFailureReason(PayrollValidationResult result) {
    // Find the highest priority failure
    for (FieldValidationResult fieldResult : result.getFieldResults()) {
      if (fieldResult.isFailed()
          && fieldResult.getComplianceLevel() == FieldValidationResult.ComplianceLevel.REGULATORY) {
        return "Regulatory compliance violation: " + fieldResult.getFieldName();
      }
    }

    for (FieldValidationResult fieldResult : result.getFieldResults()) {
      if (fieldResult.isFailed()) {
        return "Business rule violation: " + fieldResult.getFieldName();
      }
    }

    return "Data quality issue";
  }

  private static String assessBusinessImpact(PayrollValidationResult result) {
    if (result.hasRegulatoryViolations()) {
      return "CRITICAL: Regulatory compliance violation may result in penalties and audit findings. "
          + "Immediate correction required to prevent compliance breach.";
    } else if (result.hasBusinessViolations()) {
      return "HIGH: Business rule violation may impact payroll processing accuracy and employee satisfaction. "
          + "Correction required before next payroll cycle.";
    } else {
      return "MEDIUM: Data quality issue may affect reporting accuracy and operational efficiency. "
          + "Correction recommended for data integrity.";
    }
  }

  // Builder pattern
  public static class Builder {
    private PayrollEmployee originalRecord;
    private PayrollValidationResult validationResult;
    private Instant failureTimestamp = Instant.now();
    private long processingLatencyMs;
    private String hrWorkflowId;
    private Priority correctionPriority = Priority.MEDIUM;
    private RiskLevel complianceRiskLevel = RiskLevel.DATA_QUALITY;
    private CorrectionStatus correctionStatus = CorrectionStatus.PENDING;
    private Integer estimatedCorrectionTimeMinutes;
    private List<String> validationErrors = new ArrayList<>();
    private List<String> complianceFlags = new ArrayList<>();
    private String hrCorrectionInstructions;
    private String primaryFailureReason;
    private String businessImpactDescription;
    private Integer correctionAttempts = 0;
    private Instant lastCorrectionAttempt;
    private String assignedHRUser;
    private Instant hrResponseDeadline;
    private String sourceSystem = "PAYROLL_PIPELINE";
    private String pipelineVersion = "1.0.0";
    private String correlationId;

    public Builder() {}

    public Builder(FailedPayrollRecord existing) {
      this.originalRecord = existing.originalRecord;
      this.validationResult = existing.validationResult;
      this.failureTimestamp = existing.failureTimestamp;
      this.processingLatencyMs = existing.processingLatencyMs;
      this.hrWorkflowId = existing.hrWorkflowId;
      this.correctionPriority = existing.correctionPriority;
      this.complianceRiskLevel = existing.complianceRiskLevel;
      this.correctionStatus = existing.correctionStatus;
      this.estimatedCorrectionTimeMinutes = existing.estimatedCorrectionTimeMinutes;
      this.validationErrors = new ArrayList<>(existing.validationErrors);
      this.complianceFlags = new ArrayList<>(existing.complianceFlags);
      this.hrCorrectionInstructions = existing.hrCorrectionInstructions;
      this.primaryFailureReason = existing.primaryFailureReason;
      this.businessImpactDescription = existing.businessImpactDescription;
      this.correctionAttempts = existing.correctionAttempts;
      this.lastCorrectionAttempt = existing.lastCorrectionAttempt;
      this.assignedHRUser = existing.assignedHRUser;
      this.hrResponseDeadline = existing.hrResponseDeadline;
      this.sourceSystem = existing.sourceSystem;
      this.pipelineVersion = existing.pipelineVersion;
      this.correlationId = existing.correlationId;
    }

    public Builder from(FailedPayrollRecord existing) {
      if (existing == null) {
        return this;
      }
      this.originalRecord = existing.originalRecord;
      this.validationResult = existing.validationResult;
      this.failureTimestamp = existing.failureTimestamp;
      this.processingLatencyMs = existing.processingLatencyMs;
      this.hrWorkflowId = existing.hrWorkflowId;
      this.correctionPriority = existing.correctionPriority;
      this.complianceRiskLevel = existing.complianceRiskLevel;
      this.correctionStatus = existing.correctionStatus;
      this.estimatedCorrectionTimeMinutes = existing.estimatedCorrectionTimeMinutes;
      this.validationErrors = new ArrayList<>(existing.validationErrors);
      this.complianceFlags = new ArrayList<>(existing.complianceFlags);
      this.hrCorrectionInstructions = existing.hrCorrectionInstructions;
      this.primaryFailureReason = existing.primaryFailureReason;
      this.businessImpactDescription = existing.businessImpactDescription;
      this.correctionAttempts = existing.correctionAttempts;
      this.lastCorrectionAttempt = existing.lastCorrectionAttempt;
      this.assignedHRUser = existing.assignedHRUser;
      this.hrResponseDeadline = existing.hrResponseDeadline;
      this.sourceSystem = existing.sourceSystem;
      this.pipelineVersion = existing.pipelineVersion;
      this.correlationId = existing.correlationId;
      return this;
    }

    public Builder originalRecord(PayrollEmployee originalRecord) {
      this.originalRecord = originalRecord;
      return this;
    }

    public Builder validationResult(PayrollValidationResult validationResult) {
      this.validationResult = validationResult;
      return this;
    }

    public Builder failureTimestamp(Instant failureTimestamp) {
      this.failureTimestamp = failureTimestamp;
      return this;
    }

    public Builder processingLatencyMs(long processingLatencyMs) {
      this.processingLatencyMs = processingLatencyMs;
      return this;
    }

    public Builder hrWorkflowId(String hrWorkflowId) {
      this.hrWorkflowId = hrWorkflowId;
      return this;
    }

    public Builder correctionPriority(Priority correctionPriority) {
      this.correctionPriority = correctionPriority;
      return this;
    }

    public Builder complianceRiskLevel(RiskLevel complianceRiskLevel) {
      this.complianceRiskLevel = complianceRiskLevel;
      return this;
    }

    public Builder correctionStatus(CorrectionStatus correctionStatus) {
      this.correctionStatus = correctionStatus;
      return this;
    }

    public Builder estimatedCorrectionTimeMinutes(Integer estimatedCorrectionTimeMinutes) {
      this.estimatedCorrectionTimeMinutes = estimatedCorrectionTimeMinutes;
      return this;
    }

    public Builder validationErrors(List<String> validationErrors) {
      this.validationErrors = new ArrayList<>(validationErrors);
      return this;
    }

    public Builder complianceFlags(List<String> complianceFlags) {
      this.complianceFlags = new ArrayList<>(complianceFlags);
      return this;
    }

    public Builder hrCorrectionInstructions(String hrCorrectionInstructions) {
      this.hrCorrectionInstructions = hrCorrectionInstructions;
      return this;
    }

    public Builder primaryFailureReason(String primaryFailureReason) {
      this.primaryFailureReason = primaryFailureReason;
      return this;
    }

    public Builder businessImpactDescription(String businessImpactDescription) {
      this.businessImpactDescription = businessImpactDescription;
      return this;
    }

    public Builder correctionAttempts(Integer correctionAttempts) {
      this.correctionAttempts = correctionAttempts;
      return this;
    }

    public Builder lastCorrectionAttempt(Instant lastCorrectionAttempt) {
      this.lastCorrectionAttempt = lastCorrectionAttempt;
      return this;
    }

    public Builder assignedHRUser(String assignedHRUser) {
      this.assignedHRUser = assignedHRUser;
      return this;
    }

    public Builder hrResponseDeadline(Instant hrResponseDeadline) {
      this.hrResponseDeadline = hrResponseDeadline;
      return this;
    }

    public Builder sourceSystem(String sourceSystem) {
      this.sourceSystem = sourceSystem;
      return this;
    }

    public Builder pipelineVersion(String pipelineVersion) {
      this.pipelineVersion = pipelineVersion;
      return this;
    }

    public Builder correlationId(String correlationId) {
      this.correlationId = correlationId;
      return this;
    }

    public FailedPayrollRecord build() {
      Objects.requireNonNull(originalRecord, "Original record is required");
      Objects.requireNonNull(validationResult, "Validation result is required");
      Objects.requireNonNull(hrWorkflowId, "HR workflow ID is required");

      return new FailedPayrollRecord(this);
    }
  }

  // Static builder method
  public static Builder builder() {
    return new Builder();
  }

  // Object methods
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FailedPayrollRecord that = (FailedPayrollRecord) obj;
    return Objects.equals(hrWorkflowId, that.hrWorkflowId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hrWorkflowId);
  }

  @Override
  public String toString() {
    return "FailedPayrollRecord{"
        + "employeeId="
        + getEmployeeId()
        + ", hrWorkflowId='"
        + hrWorkflowId
        + '\''
        + ", priority="
        + correctionPriority
        + ", status="
        + correctionStatus
        + ", errorCount="
        + validationErrors.size()
        + '}';
  }
}
