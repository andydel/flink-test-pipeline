package com.flinkpipeline.payroll.models;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable compliance audit log entry for regulatory and operational auditing. Provides
 * comprehensive audit trail for all payroll data processing operations, PII access events, and
 * compliance-related activities with tamper-evident features.
 */
public class ComplianceAuditLog {

  public enum AuditType {
    // Core payroll operations
    PAYROLL_RECORD_PROCESSED,
    PAYROLL_RECORD_VALIDATED,
    PAYROLL_RECORD_FAILED,

    // PII and data access
    PII_FIELD_ACCESSED,
    PII_FIELD_ENCRYPTED,
    PII_FIELD_DECRYPTED,

    // Validation and business logic
    VALIDATION_RULE_APPLIED,
    VALIDATION_DECISION_MADE,
    DUPLICATE_DETECTION_PERFORMED,

    // HR workflow operations
    HR_TICKET_CREATED,
    HR_TICKET_ASSIGNED,
    HR_TICKET_ACKNOWLEDGED,
    HR_TICKET_RESOLVED,
    HR_CORRECTION_ATTEMPTED,

    // System and compliance operations
    COMPLIANCE_VIOLATION_DETECTED,
    COMPLIANCE_INVESTIGATION_INITIATED,
    DATA_RETENTION_POLICY_APPLIED,
    AUDIT_LOG_CREATED,

    // External integrations
    EXTERNAL_SYSTEM_NOTIFICATION,
    EXTERNAL_COMPLIANCE_ALERT,
    REGULATORY_REPORT_GENERATED
  }

  public enum ComplianceStatus {
    COMPLIANT, // Operation meets all compliance requirements
    VIOLATION, // Compliance violation detected
    REVIEW_REQUIRED, // Manual review needed for compliance assessment
    PENDING_VALIDATION, // Awaiting compliance validation
    EXCEPTION_APPROVED // Approved exception to compliance rule
  }

  public enum DataClassification {
    PUBLIC, // No sensitivity restrictions
    INTERNAL, // Internal company use only
    CONFIDENTIAL, // Confidential business information
    RESTRICTED, // Highly sensitive, limited access
    PII, // Personally Identifiable Information
    PHI, // Protected Health Information
    FINANCIAL // Financial and payroll data
  }

  // Core audit information (immutable)
  private final String auditId;
  private final Integer employeeId;
  private final Instant auditTimestamp;
  private final AuditType auditType;
  private final ComplianceStatus complianceStatus;

  // Operation details
  private final String userId;
  private final String sessionId;
  private final String operationDetails;
  private final String systemComponent;
  private final String sourceIPAddress;
  private final String userAgent;

  // PII and data access tracking
  private final List<String> piiFieldsAccessed;
  private final DataClassification dataClassification;
  private final String accessPurpose;
  private final String accessJustification;
  private final boolean authorizedAccess;

  // Compliance and risk information
  private final String complianceFramework;
  private final String riskLevel;
  private final String businessJustification;
  private final List<String> complianceFlags;

  // Data retention and lifecycle
  private final Instant retentionExpires;
  private final String retentionPolicy;
  private final boolean immutableRecord;

  // Tamper-evident features
  private final String auditHash;
  private final String digitalSignature;
  private final String previousAuditHash;

  // System metadata
  private final String pipelineVersion;
  private final String correlationId;
  private final String transactionId;
  private final long processingLatencyMs;

  // Constructor (private - use Builder)
  private ComplianceAuditLog(Builder builder) {
    this.auditId = builder.auditId;
    this.employeeId = builder.employeeId;
    this.auditTimestamp = builder.auditTimestamp;
    this.auditType = builder.auditType;
    this.complianceStatus = builder.complianceStatus;
    this.userId = builder.userId;
    this.sessionId = builder.sessionId;
    this.operationDetails = builder.operationDetails;
    this.systemComponent = builder.systemComponent;
    this.sourceIPAddress = builder.sourceIPAddress;
    this.userAgent = builder.userAgent;
    this.piiFieldsAccessed =
        builder.piiFieldsAccessed != null
            ? new ArrayList<>(builder.piiFieldsAccessed)
            : new ArrayList<>();
    this.dataClassification = builder.dataClassification;
    this.accessPurpose = builder.accessPurpose;
    this.accessJustification = builder.accessJustification;
    this.authorizedAccess = builder.authorizedAccess;
    this.complianceFramework = builder.complianceFramework;
    this.riskLevel = builder.riskLevel;
    this.businessJustification = builder.businessJustification;
    this.complianceFlags =
        builder.complianceFlags != null
            ? new ArrayList<>(builder.complianceFlags)
            : new ArrayList<>();
    this.retentionExpires = builder.retentionExpires;
    this.retentionPolicy = builder.retentionPolicy;
    this.immutableRecord = builder.immutableRecord;
    this.auditHash = builder.auditHash;
    this.digitalSignature = builder.digitalSignature;
    this.previousAuditHash = builder.previousAuditHash;
    this.pipelineVersion = builder.pipelineVersion;
    this.correlationId = builder.correlationId;
    this.transactionId = builder.transactionId;
    this.processingLatencyMs = builder.processingLatencyMs;
  }

  // Static factory methods for common audit scenarios
  public static ComplianceAuditLog createPayrollProcessingAudit(
      Integer employeeId, String userId, String operationDetails) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.PAYROLL_RECORD_PROCESSED)
        .userId(userId)
        .operationDetails(operationDetails)
        .complianceStatus(ComplianceStatus.COMPLIANT)
        .dataClassification(DataClassification.FINANCIAL)
        .systemComponent("PayrollValidationOperator")
        .authorizedAccess(true)
        .build();
  }

  public static ComplianceAuditLog createPIIAccessAudit(
      Integer employeeId, String userId, List<String> piiFields, String purpose) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.PII_FIELD_ACCESSED)
        .userId(userId)
        .operationDetails("PII fields accessed: " + String.join(", ", piiFields))
        .piiFieldsAccessed(piiFields)
        .accessPurpose(purpose)
        .dataClassification(DataClassification.PII)
        .complianceStatus(ComplianceStatus.COMPLIANT)
        .systemComponent("PIIEncryptionService")
        .authorizedAccess(true)
        .retentionPolicy("PII_ACCESS_RETENTION")
        .build();
  }

  public static ComplianceAuditLog createComplianceViolationAudit(
      Integer employeeId, String violationType, String violationDetails) {
    return createComplianceViolationAudit(employeeId, violationType, violationDetails, "HIGH");
  }

  public static ComplianceAuditLog createComplianceViolationAudit(
      Integer employeeId, String violationType, String violationDetails, String severity) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.COMPLIANCE_VIOLATION_DETECTED)
        .userId("SYSTEM")
        .operationDetails("Compliance violation detected: " + violationDetails)
        .complianceStatus(ComplianceStatus.VIOLATION)
        .riskLevel(severity != null ? severity : "HIGH")
        .dataClassification(DataClassification.RESTRICTED)
        .systemComponent("ComplianceAuditor")
        .complianceFlag(violationType)
        .retentionPolicy("COMPLIANCE_VIOLATION_RETENTION")
        .build();
  }

  public static ComplianceAuditLog createSystemErrorAudit(
      Integer employeeId, String component, String errorMessage, Map<String, String> metadata) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.AUDIT_LOG_CREATED)
        .userId("SYSTEM")
        .operationDetails(buildOperationDetails(errorMessage, metadata))
        .systemComponent(component)
        .complianceStatus(ComplianceStatus.VIOLATION)
        .riskLevel("HIGH")
        .authorizedAccess(false)
        .dataClassification(DataClassification.INTERNAL)
        .build();
  }

  public static ComplianceAuditLog createDataProcessingAudit(
      Integer employeeId, String component, String description, Map<String, ?> metadata) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.AUDIT_LOG_CREATED)
        .userId("SYSTEM")
        .operationDetails(buildOperationDetails(description, metadata))
        .systemComponent(component)
        .complianceStatus(ComplianceStatus.COMPLIANT)
        .dataClassification(DataClassification.INTERNAL)
        .authorizedAccess(true)
        .build();
  }

  public static ComplianceAuditLog createHRWorkflowAudit(
      Integer employeeId, String userId, String hrAction, String workflowId) {
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.HR_TICKET_CREATED)
        .userId(userId)
        .operationDetails("HR workflow action: " + hrAction + " (Workflow ID: " + workflowId + ")")
        .complianceStatus(ComplianceStatus.COMPLIANT)
        .dataClassification(DataClassification.CONFIDENTIAL)
        .systemComponent("HRWorkflowService")
        .correlationId(workflowId)
        .authorizedAccess(true)
        .build();
  }

  public static ComplianceAuditLog createValidationAudit(
      Integer employeeId,
      String operation,
      String status,
      List<FieldValidationResult> fieldResults) {
    String details =
        String.format(
            "Validation %s: %d fields checked",
            status, fieldResults != null ? fieldResults.size() : 0);
    return new Builder()
        .employeeId(employeeId)
        .auditType(AuditType.PAYROLL_RECORD_VALIDATED)
        .userId("SYSTEM")
        .operationDetails(details)
        .complianceStatus(
            status.equals("PASSED") ? ComplianceStatus.COMPLIANT : ComplianceStatus.VIOLATION)
        .dataClassification(DataClassification.FINANCIAL)
        .systemComponent(operation)
        .authorizedAccess(true)
        .build();
  }

  // Getters (all immutable)
  public String getAuditId() {
    return auditId;
  }

  public Integer getEmployeeId() {
    return employeeId;
  }

  public Instant getAuditTimestamp() {
    return auditTimestamp;
  }

  public AuditType getAuditType() {
    return auditType;
  }

  public ComplianceStatus getComplianceStatus() {
    return complianceStatus;
  }

  public String getUserId() {
    return userId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getOperationDetails() {
    return operationDetails;
  }

  public String getSystemComponent() {
    return systemComponent;
  }

  public String getSourceIPAddress() {
    return sourceIPAddress;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public List<String> getPiiFieldsAccessed() {
    return new ArrayList<>(piiFieldsAccessed);
  }

  public DataClassification getDataClassification() {
    return dataClassification;
  }

  public String getAccessPurpose() {
    return accessPurpose;
  }

  public String getAccessJustification() {
    return accessJustification;
  }

  public boolean isAuthorizedAccess() {
    return authorizedAccess;
  }

  public String getComplianceFramework() {
    return complianceFramework;
  }

  public String getRiskLevel() {
    return riskLevel;
  }

  public String getBusinessJustification() {
    return businessJustification;
  }

  public List<String> getComplianceFlags() {
    return new ArrayList<>(complianceFlags);
  }

  public Instant getRetentionExpires() {
    return retentionExpires;
  }

  public String getRetentionPolicy() {
    return retentionPolicy;
  }

  public boolean isImmutableRecord() {
    return immutableRecord;
  }

  public String getAuditHash() {
    return auditHash;
  }

  public String getDigitalSignature() {
    return digitalSignature;
  }

  public String getPreviousAuditHash() {
    return previousAuditHash;
  }

  public String getPipelineVersion() {
    return pipelineVersion;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public long getProcessingLatencyMs() {
    return processingLatencyMs;
  }

  // Business logic methods
  public boolean isViolation() {
    return complianceStatus == ComplianceStatus.VIOLATION;
  }

  public boolean involvesPII() {
    return dataClassification == DataClassification.PII || !piiFieldsAccessed.isEmpty();
  }

  public boolean isExpired() {
    return retentionExpires != null && Instant.now().isAfter(retentionExpires);
  }

  public boolean isHighRisk() {
    return "HIGH".equals(riskLevel) || "CRITICAL".equals(riskLevel);
  }

  public boolean requiresImmediateAttention() {
    return isViolation() && isHighRisk();
  }

  public long getRetentionDaysRemaining() {
    if (retentionExpires == null) return Long.MAX_VALUE;
    long secondsRemaining = retentionExpires.getEpochSecond() - Instant.now().getEpochSecond();
    return Math.max(0, secondsRemaining / (24 * 60 * 60));
  }

  // Tamper-evident verification
  public boolean verifyIntegrity() {
    String recalculatedHash = calculateHash();
    return auditHash != null && auditHash.equals(recalculatedHash);
  }

  private String calculateHash() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      String dataToHash = auditId + auditTimestamp + auditType + operationDetails + userId;
      byte[] hashBytes = digest.digest(dataToHash.getBytes());
      StringBuilder sb = new StringBuilder();
      for (byte b : hashBytes) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }

  private static String buildOperationDetails(String description, Map<String, ?> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return description;
    }
    return description + " | metadata=" + metadata;
  }

  // Builder pattern
  public static class Builder {
    private String auditId = UUID.randomUUID().toString();
    private Integer employeeId;
    private Instant auditTimestamp = Instant.now();
    private AuditType auditType;
    private ComplianceStatus complianceStatus = ComplianceStatus.COMPLIANT;
    private String userId = "SYSTEM";
    private String sessionId;
    private String operationDetails;
    private String systemComponent;
    private String sourceIPAddress;
    private String userAgent;
    private List<String> piiFieldsAccessed;
    private DataClassification dataClassification = DataClassification.INTERNAL;
    private String accessPurpose;
    private String accessJustification;
    private boolean authorizedAccess = true;
    private String complianceFramework = "SOX_PAYROLL";
    private String riskLevel = "MEDIUM";
    private String businessJustification;
    private List<String> complianceFlags = new ArrayList<>();
    private Instant retentionExpires =
        Instant.now().plus(java.time.Duration.ofDays(7 * 365)); // 7 years default
    private String retentionPolicy = "STANDARD_PAYROLL_RETENTION";
    private boolean immutableRecord = true;
    private String auditHash;
    private String digitalSignature;
    private String previousAuditHash;
    private String pipelineVersion = "1.0.0";
    private String correlationId;
    private String transactionId;
    private long processingLatencyMs;

    public Builder auditId(String auditId) {
      this.auditId = auditId;
      return this;
    }

    public Builder employeeId(Integer employeeId) {
      this.employeeId = employeeId;
      return this;
    }

    public Builder auditTimestamp(Instant auditTimestamp) {
      this.auditTimestamp = auditTimestamp;
      return this;
    }

    public Builder auditType(AuditType auditType) {
      this.auditType = auditType;
      return this;
    }

    public Builder complianceStatus(ComplianceStatus complianceStatus) {
      this.complianceStatus = complianceStatus;
      return this;
    }

    public Builder userId(String userId) {
      this.userId = userId;
      return this;
    }

    public Builder sessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public Builder operationDetails(String operationDetails) {
      this.operationDetails = operationDetails;
      return this;
    }

    public Builder systemComponent(String systemComponent) {
      this.systemComponent = systemComponent;
      return this;
    }

    public Builder sourceIPAddress(String sourceIPAddress) {
      this.sourceIPAddress = sourceIPAddress;
      return this;
    }

    public Builder userAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder piiFieldsAccessed(List<String> piiFieldsAccessed) {
      this.piiFieldsAccessed = piiFieldsAccessed;
      return this;
    }

    public Builder dataClassification(DataClassification dataClassification) {
      this.dataClassification = dataClassification;
      return this;
    }

    public Builder accessPurpose(String accessPurpose) {
      this.accessPurpose = accessPurpose;
      return this;
    }

    public Builder accessJustification(String accessJustification) {
      this.accessJustification = accessJustification;
      return this;
    }

    public Builder authorizedAccess(boolean authorizedAccess) {
      this.authorizedAccess = authorizedAccess;
      return this;
    }

    public Builder complianceFramework(String complianceFramework) {
      this.complianceFramework = complianceFramework;
      return this;
    }

    public Builder riskLevel(String riskLevel) {
      this.riskLevel = riskLevel;
      return this;
    }

    public Builder businessJustification(String businessJustification) {
      this.businessJustification = businessJustification;
      return this;
    }

    public Builder complianceFlags(List<String> complianceFlags) {
      this.complianceFlags = complianceFlags;
      return this;
    }

    public Builder complianceFlag(String flag) {
      this.complianceFlags.add(flag);
      return this;
    }

    public Builder retentionExpires(Instant retentionExpires) {
      this.retentionExpires = retentionExpires;
      return this;
    }

    public Builder retentionPolicy(String retentionPolicy) {
      this.retentionPolicy = retentionPolicy;
      return this;
    }

    public Builder immutableRecord(boolean immutableRecord) {
      this.immutableRecord = immutableRecord;
      return this;
    }

    public Builder auditHash(String auditHash) {
      this.auditHash = auditHash;
      return this;
    }

    public Builder digitalSignature(String digitalSignature) {
      this.digitalSignature = digitalSignature;
      return this;
    }

    public Builder previousAuditHash(String previousAuditHash) {
      this.previousAuditHash = previousAuditHash;
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

    public Builder transactionId(String transactionId) {
      this.transactionId = transactionId;
      return this;
    }

    public Builder processingLatencyMs(long processingLatencyMs) {
      this.processingLatencyMs = processingLatencyMs;
      return this;
    }

    public ComplianceAuditLog build() {
      Objects.requireNonNull(auditType, "Audit type is required");
      Objects.requireNonNull(operationDetails, "Operation details are required");

      // Calculate hash if not provided
      ComplianceAuditLog tempLog = new ComplianceAuditLog(this);
      if (this.auditHash == null) {
        this.auditHash = tempLog.calculateHash();
      }

      return new ComplianceAuditLog(this);
    }
  }

  // Object methods (immutable)
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ComplianceAuditLog that = (ComplianceAuditLog) obj;
    return Objects.equals(auditId, that.auditId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auditId);
  }

  @Override
  public String toString() {
    return "ComplianceAuditLog{"
        + "auditId='"
        + auditId
        + '\''
        + ", employeeId="
        + employeeId
        + ", auditType="
        + auditType
        + ", complianceStatus="
        + complianceStatus
        + ", userId='"
        + userId
        + '\''
        + ", timestamp="
        + auditTimestamp
        + '}';
  }
}
