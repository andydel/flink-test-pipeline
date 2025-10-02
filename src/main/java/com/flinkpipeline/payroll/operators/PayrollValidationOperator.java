package com.flinkpipeline.payroll.operators;

import com.flinkpipeline.payroll.models.ComplianceAuditLog;
import com.flinkpipeline.payroll.models.FailedPayrollRecord;
import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import com.flinkpipeline.payroll.models.PayrollQualityRule;
import com.flinkpipeline.payroll.models.PayrollValidationResult;
import com.flinkpipeline.payroll.validation.rules.DuplicateDetectionRule;
import com.flinkpipeline.payroll.validation.rules.EmailValidationRule;
import com.flinkpipeline.payroll.validation.rules.NameFormatValidationRule;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Flink operator for comprehensive payroll data validation.
 * Orchestrates multiple validation rules, handles compliance auditing,
 * and routes valid/invalid records to appropriate downstream processing.
 *
 * Features:
 * - Multi-rule validation with configurable rule sets
 * - PII compliance tracking and audit logging
 * - Federal employment law validation (SSN, age, wage compliance)
 * - Duplicate detection with windowed processing
 * - Performance metrics and validation statistics
 * - Side output for failed records and audit logs
 */
public class PayrollValidationOperator extends ProcessFunction<PayrollEmployee, PayrollEmployee> {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollValidationOperator.class);

  // Output tags for side outputs
  public static final OutputTag<FailedPayrollRecord> FAILED_RECORDS_TAG =
      new OutputTag<FailedPayrollRecord>("failed-records") {};
  public static final OutputTag<ComplianceAuditLog> AUDIT_LOGS_TAG =
      new OutputTag<ComplianceAuditLog>("audit-logs") {};

  // Validation rules
  private final List<PayrollQualityRule> configuredRules;
  private final boolean enableDuplicateDetection;
  private final boolean enableComplianceAuditing;
  private final boolean enablePIIEncryption;
  private final Duration duplicateDetectionWindow;

  // Rule instances
  private transient NameFormatValidationRule nameValidator;
  private transient EmailValidationRule emailValidator;
  private transient DuplicateDetectionRule duplicateDetector;

  // Performance tracking
  private transient AtomicLong totalRecordsProcessed;
  private transient AtomicLong totalValidRecords;
  private transient AtomicLong totalInvalidRecords;
  private transient AtomicLong totalDuplicatesDetected;
  private transient AtomicLong totalComplianceViolations;

  // State for duplicate detection window
  private transient ValueState<Long> windowStart;

  // Federal compliance patterns
  private static final Pattern SSN_PATTERN = Pattern.compile("^\\d{3}-\\d{2}-\\d{4}$");
  private static final Pattern ITIN_PATTERN = Pattern.compile("^9\\d{2}-\\d{2}-\\d{4}$");
  private static final int MIN_EMPLOYMENT_AGE = 16;
  private static final int MAX_EMPLOYMENT_AGE = 75;
  private static final int MIN_WAGE_CENTS = 725; // $7.25 federal minimum wage
  private static final int MAX_REASONABLE_WAGE_CENTS = 50000; // $500/hour reasonable max

  // Constructor
  public PayrollValidationOperator() {
    this(getDefaultRules(), true, true, false, Duration.ofMinutes(60));
  }

  public PayrollValidationOperator(List<PayrollQualityRule> rules,
                                  boolean enableDuplicateDetection,
                                  boolean enableComplianceAuditing,
                                  boolean enablePIIEncryption,
                                  Duration duplicateDetectionWindow) {
    this.configuredRules = new ArrayList<>(rules);
    this.enableDuplicateDetection = enableDuplicateDetection;
    this.enableComplianceAuditing = enableComplianceAuditing;
    this.enablePIIEncryption = enablePIIEncryption;
    this.duplicateDetectionWindow = duplicateDetectionWindow;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    LOG.info("Initializing PayrollValidationOperator with {} rules", configuredRules.size());

    // Initialize validation rule instances
    this.nameValidator = new NameFormatValidationRule();
    this.emailValidator = new EmailValidationRule();

    if (enableDuplicateDetection) {
      this.duplicateDetector = new DuplicateDetectionRule(duplicateDetectionWindow);
      LOG.info("Duplicate detection enabled with window: {}", duplicateDetectionWindow);
    }

    // Initialize performance counters
    this.totalRecordsProcessed = new AtomicLong(0);
    this.totalValidRecords = new AtomicLong(0);
    this.totalInvalidRecords = new AtomicLong(0);
    this.totalDuplicatesDetected = new AtomicLong(0);
    this.totalComplianceViolations = new AtomicLong(0);

    // Initialize state for windowing
    ValueStateDescriptor<Long> windowDescriptor =
        new ValueStateDescriptor<>("window-start", Long.class);
    this.windowStart = getRuntimeContext().getState(windowDescriptor);

    LOG.info("PayrollValidationOperator initialized successfully");
  }

  @Override
  public void processElement(PayrollEmployee record, Context context,
                           Collector<PayrollEmployee> out) throws Exception {

    long startTime = System.currentTimeMillis();
    totalRecordsProcessed.incrementAndGet();

    LOG.debug("Processing employee record: ID={}, Name={} {}",
             record.getEmployeeId(), record.getFirstName(), record.getLastName());

    try {
      // Create validation result container
      PayrollValidationResult.Builder resultBuilder = PayrollValidationResult.builder()
          .employee(record)
          .validationTimestamp(Instant.ofEpochMilli(context.timestamp()));

      List<FieldValidationResult> validationResults = new ArrayList<>();
      boolean hasComplianceViolations = false;

      // Execute configured validation rules
      for (PayrollQualityRule rule : configuredRules) {
        FieldValidationResult ruleResult = executeValidationRule(record, rule);
        validationResults.add(ruleResult);

        if (ruleResult.isFailed() && isComplianceViolation(rule)) {
          hasComplianceViolations = true;
          totalComplianceViolations.incrementAndGet();
        }
      }

      // Execute built-in validation rules
      validationResults.addAll(executeBuiltInValidations(record));

      // Execute duplicate detection if enabled
      if (enableDuplicateDetection && duplicateDetector != null) {
        FieldValidationResult duplicateResult = duplicateDetector.checkDuplicate(record);
        if (duplicateResult.isFailed()) {
          validationResults.add(duplicateResult);
          totalDuplicatesDetected.incrementAndGet();
        }
      }

      // Build final validation result
      PayrollValidationResult validationResult = resultBuilder
          .fieldResults(validationResults)
          .overallResult(determineOverallResult(validationResults))
          .processingLatencyMs(System.currentTimeMillis() - startTime)
          .build();

      // Generate compliance audit logs if enabled
      if (enableComplianceAuditing) {
        generateComplianceAuditLogs(record, validationResult, context);
      }

      // Route record based on validation outcome
      if (validationResult.isValid()) {
        // Process successful validation
        totalValidRecords.incrementAndGet();

        // Apply PII encryption if enabled
        PayrollEmployee processedRecord = enablePIIEncryption ?
            applyPIIEncryption(record) : record;

        out.collect(processedRecord);

        LOG.debug("Record passed validation: ID={}", record.getEmployeeId());

      } else {
        // Handle validation failure
        totalInvalidRecords.incrementAndGet();

        FailedPayrollRecord failedRecord = FailedPayrollRecord.fromValidationResult(record, validationResult);
        context.output(FAILED_RECORDS_TAG, failedRecord);

        LOG.warn("Record failed validation: ID={}, Errors={}",
                record.getEmployeeId(), validationResult.getFailedFieldsCount());
      }

      // Log performance metrics periodically
      if (totalRecordsProcessed.get() % 1000 == 0) {
        logPerformanceMetrics();
      }

    } catch (Exception e) {
      LOG.error("Failed to process payroll record: ID={}", record.getEmployeeId(), e);

      // Create failure record for processing errors
      FailedPayrollRecord errorRecord = FailedPayrollRecord.builder()
          .originalRecord(record)
          .failureTimestamp(Instant.now())
          .hrWorkflowId("ERROR-" + System.currentTimeMillis())
          .validationErrors(Arrays.asList("Processing error: " + e.getMessage()))
          .correctionPriority("HIGH")
          .processingLatencyMs(System.currentTimeMillis() - startTime)
          .build();

      context.output(FAILED_RECORDS_TAG, errorRecord);
      totalInvalidRecords.incrementAndGet();
    }
  }

  /**
   * Execute a configured validation rule
   */
  private FieldValidationResult executeValidationRule(PayrollEmployee record, PayrollQualityRule rule) {
    try {
      switch (rule.getRuleType()) {
        case FORMAT:
          return validateFormat(record, rule);
        case RANGE:
          return validateRange(record, rule);
        case REQUIRED:
          return validateRequired(record, rule);
        case REGEX:
          return validateRegex(record, rule);
        case BUSINESS_LOGIC:
          return validateBusinessLogic(record, rule);
        default:
          return FieldValidationResult.failure(
              rule.getFieldName(),
              rule.getRuleName(),
              "Unknown rule type: " + rule.getRuleType(),
              "Check rule configuration",
              rule.getComplianceLevel()
          );
      }
    } catch (Exception e) {
      LOG.error("Error executing rule {}: {}", rule.getRuleName(), e.getMessage());
      return FieldValidationResult.failure(
          rule.getFieldName(),
          rule.getRuleName(),
          "Rule execution error: " + e.getMessage(),
          "Check rule implementation and data format",
          rule.getComplianceLevel()
      );
    }
  }

  /**
   * Execute built-in validation rules using dedicated validators
   */
  private List<FieldValidationResult> executeBuiltInValidations(PayrollEmployee record) {
    List<FieldValidationResult> results = new ArrayList<>();

    // Name validations
    if (record.getFirstName() != null) {
      results.add(nameValidator.validateFirstName(record.getFirstName()));
    }
    if (record.getLastName() != null) {
      results.add(nameValidator.validateLastName(record.getLastName()));
    }
    if (record.getFirstName() != null && record.getLastName() != null) {
      results.add(nameValidator.validateFullName(record.getFirstName(), record.getLastName()));
    }

    // Email validation
    if (record.getEmail() != null) {
      results.add(emailValidator.validateEmail(record.getEmail()));
    }

    // Federal compliance validations
    results.add(validateSSN(record));
    results.add(validateAge(record));
    results.add(validateWage(record));

    return results;
  }

  /**
   * Validate Social Security Number format and compliance
   */
  private FieldValidationResult validateSSN(PayrollEmployee record) {
    String ssn = record.getSsn();

    if (ssn == null || ssn.trim().isEmpty()) {
      return FieldValidationResult.failure(
          "ssn",
          "SSN Required Validation",
          "Social Security Number is required for payroll processing",
          "Enter valid SSN in format: XXX-XX-XXXX",
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    String trimmedSSN = ssn.trim();

    // Check format
    if (!SSN_PATTERN.matcher(trimmedSSN).matches() && !ITIN_PATTERN.matcher(trimmedSSN).matches()) {
      return FieldValidationResult.failure(
          "ssn",
          "SSN Format Validation",
          "Invalid SSN/ITIN format",
          "Use format XXX-XX-XXXX (SSN) or 9XX-XX-XXXX (ITIN)",
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    // Check for invalid SSN patterns
    if (isInvalidSSN(trimmedSSN)) {
      return FieldValidationResult.failure(
          "ssn",
          "SSN Validity Validation",
          "SSN contains invalid number pattern",
          "Verify SSN is valid and issued by Social Security Administration",
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    return FieldValidationResult.success("ssn", "SSN Validation");
  }

  /**
   * Validate employee age for employment eligibility
   */
  private FieldValidationResult validateAge(PayrollEmployee record) {
    Integer age = record.getAge();

    if (age == null) {
      return FieldValidationResult.failure(
          "age",
          "Age Required Validation",
          "Employee age is required for employment eligibility verification",
          "Enter valid age between " + MIN_EMPLOYMENT_AGE + " and " + MAX_EMPLOYMENT_AGE,
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    if (age < MIN_EMPLOYMENT_AGE) {
      return FieldValidationResult.failure(
          "age",
          "Minimum Age Validation",
          "Employee age below federal minimum employment age (" + MIN_EMPLOYMENT_AGE + ")",
          "Verify age and ensure compliance with child labor laws",
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    if (age > MAX_EMPLOYMENT_AGE) {
      return FieldValidationResult.failure(
          "age",
          "Maximum Age Validation",
          "Employee age exceeds reasonable employment range (" + MAX_EMPLOYMENT_AGE + ")",
          "Verify age accuracy",
          FieldValidationResult.ComplianceLevel.BUSINESS
      );
    }

    return FieldValidationResult.success("age", "Age Validation");
  }

  /**
   * Validate hourly rate for wage compliance
   */
  private FieldValidationResult validateWage(PayrollEmployee record) {
    Integer hourlyRateCents = record.getHourlyRate();

    if (hourlyRateCents == null) {
      return FieldValidationResult.failure(
          "hourly_rate_cents",
          "Wage Required Validation",
          "Hourly rate is required for payroll processing",
          "Enter valid hourly rate in cents",
          FieldValidationResult.ComplianceLevel.BUSINESS
      );
    }

    if (hourlyRateCents < MIN_WAGE_CENTS) {
      return FieldValidationResult.failure(
          "hourly_rate_cents",
          "Minimum Wage Validation",
          "Hourly rate below federal minimum wage ($" + (MIN_WAGE_CENTS / 100.0) + "/hour)",
          "Ensure wage meets federal minimum wage requirements",
          FieldValidationResult.ComplianceLevel.REGULATORY
      );
    }

    if (hourlyRateCents > MAX_REASONABLE_WAGE_CENTS) {
      return FieldValidationResult.failure(
          "hourly_rate_cents",
          "Maximum Wage Validation",
          "Hourly rate exceeds reasonable maximum ($" + (MAX_REASONABLE_WAGE_CENTS / 100.0) + "/hour)",
          "Verify wage amount is correct",
          FieldValidationResult.ComplianceLevel.BUSINESS
      );
    }

    return FieldValidationResult.success("hourly_rate_cents", "Wage Validation");
  }

  /**
   * Apply specific validation rule types
   */
  private FieldValidationResult validateFormat(PayrollEmployee record, PayrollQualityRule rule) {
    Object fieldValue = getFieldValue(record, rule.getFieldName());
    if (fieldValue == null) {
      return FieldValidationResult.failure(
          rule.getFieldName(),
          rule.getRuleName(),
          "Field is null",
          "Enter valid " + rule.getFieldName(),
          rule.getComplianceLevel()
      );
    }

    String pattern = rule.getValidationExpression();
    if (pattern != null && !Pattern.matches(pattern, fieldValue.toString())) {
      return FieldValidationResult.failure(
          rule.getFieldName(),
          rule.getRuleName(),
          "Field format validation failed",
          "Use valid format: " + rule.getExpectedFormat(),
          rule.getComplianceLevel()
      );
    }

    return FieldValidationResult.success(rule.getFieldName(), rule.getRuleName());
  }

  private FieldValidationResult validateRange(PayrollEmployee record, PayrollQualityRule rule) {
    Object fieldValue = getFieldValue(record, rule.getFieldName());
    if (fieldValue == null) {
      return FieldValidationResult.success(rule.getFieldName(), rule.getRuleName()); // Null allowed for range checks
    }

    if (fieldValue instanceof Number) {
      double value = ((Number) fieldValue).doubleValue();
      String[] rangeParts = rule.getValidationExpression().split(",");

      if (rangeParts.length == 2) {
        double min = Double.parseDouble(rangeParts[0]);
        double max = Double.parseDouble(rangeParts[1]);

        if (value < min || value > max) {
          return FieldValidationResult.failure(
              rule.getFieldName(),
              rule.getRuleName(),
              "Value outside allowed range [" + min + ", " + max + "]",
              "Enter value between " + min + " and " + max,
              rule.getComplianceLevel()
          );
        }
      }
    }

    return FieldValidationResult.success(rule.getFieldName(), rule.getRuleName());
  }

  private FieldValidationResult validateRequired(PayrollEmployee record, PayrollQualityRule rule) {
    Object fieldValue = getFieldValue(record, rule.getFieldName());

    if (fieldValue == null || (fieldValue instanceof String && ((String) fieldValue).trim().isEmpty())) {
      return FieldValidationResult.failure(
          rule.getFieldName(),
          rule.getRuleName(),
          "Required field is missing or empty",
          "Enter valid " + rule.getFieldName(),
          rule.getComplianceLevel()
      );
    }

    return FieldValidationResult.success(rule.getFieldName(), rule.getRuleName());
  }

  private FieldValidationResult validateRegex(PayrollEmployee record, PayrollQualityRule rule) {
    return validateFormat(record, rule); // Same logic as format validation
  }

  private FieldValidationResult validateBusinessLogic(PayrollEmployee record, PayrollQualityRule rule) {
    // Extensible business logic validation - can be enhanced with scripting engine
    return FieldValidationResult.success(rule.getFieldName(), rule.getRuleName());
  }

  /**
   * Determine overall validation result
   */
  private PayrollValidationResult.ValidationStatus determineOverallResult(List<FieldValidationResult> results) {
    boolean hasFailures = results.stream().anyMatch(FieldValidationResult::isFailed);
    boolean hasWarnings = results.stream().anyMatch(result ->
        !result.isFailed() && result.getComplianceLevel() == FieldValidationResult.ComplianceLevel.BUSINESS);

    if (hasFailures) {
      return PayrollValidationResult.ValidationStatus.FAILED;
    } else if (hasWarnings) {
      return PayrollValidationResult.ValidationStatus.WARNING;
    } else {
      return PayrollValidationResult.ValidationStatus.PASSED;
    }
  }

  /**
   * Generate compliance audit logs
   */
  private void generateComplianceAuditLogs(PayrollEmployee record, PayrollValidationResult result,
                                         Context context) {

    // Log PII access
    List<String> piiFields = Arrays.asList("ssn", "email");
    ComplianceAuditLog piiAudit = ComplianceAuditLog.createPIIAccessAudit(
        record.getEmployeeId(),
        "flink-validation-operator",
        piiFields,
        "payroll_validation"
    );
    context.output(AUDIT_LOGS_TAG, piiAudit);

    // Log validation outcome
    ComplianceAuditLog validationAudit = ComplianceAuditLog.createValidationAudit(
        record.getEmployeeId(),
        "payroll_validation",
        result.isValid() ? "PASSED" : "FAILED",
        result.getFieldResults()
    );
    context.output(AUDIT_LOGS_TAG, validationAudit);

    // Log compliance violations if any
    if (!result.isValid()) {
      ComplianceAuditLog violationAudit = ComplianceAuditLog.createComplianceViolationAudit(
          record.getEmployeeId(),
          "DATA_QUALITY_VIOLATION",
          String.valueOf(result.getFailedFieldsCount()) + " validation failures",
          "HIGH"
      );
      context.output(AUDIT_LOGS_TAG, violationAudit);
    }
  }

  /**
   * Apply PII encryption (placeholder implementation)
   */
  private PayrollEmployee applyPIIEncryption(PayrollEmployee record) {
    // In real implementation, this would encrypt SSN and other PII fields
    // For now, return the record as-is
    return record;
  }

  /**
   * Helper methods
   */
  private Object getFieldValue(PayrollEmployee record, String fieldName) {
    switch (fieldName.toLowerCase()) {
      case "employee_id": return record.getEmployeeId();
      case "first_name": return record.getFirstName();
      case "last_name": return record.getLastName();
      case "age": return record.getAge();
      case "ssn": return record.getSsn();
      case "hourly_rate_cents": return record.getHourlyRate();
      case "gender": return record.getGender();
      case "email": return record.getEmail();
      default: return null;
    }
  }

  private boolean isComplianceViolation(PayrollQualityRule rule) {
    return rule.getComplianceLevel() == PayrollQualityRule.ComplianceLevel.REGULATORY;
  }

  private boolean isInvalidSSN(String ssn) {
    // Check for well-known invalid SSN patterns
    String[] invalidPatterns = {
        "000-00-0000", "123-45-6789", "666-00-0000", "900-00-0000"
    };

    for (String pattern : invalidPatterns) {
      if (ssn.equals(pattern)) {
        return true;
      }
    }

    // Check for area number 000 or 666
    String areaNumber = ssn.substring(0, 3);
    return "000".equals(areaNumber) || "666".equals(areaNumber);
  }

  private void logPerformanceMetrics() {
    long total = totalRecordsProcessed.get();
    long valid = totalValidRecords.get();
    long invalid = totalInvalidRecords.get();
    long duplicates = totalDuplicatesDetected.get();
    long violations = totalComplianceViolations.get();

    double validationRate = total > 0 ? (double) valid / total * 100 : 0;

    LOG.info("Validation Metrics - Total: {}, Valid: {}, Invalid: {}, Duplicates: {}, " +
             "Violations: {}, Success Rate: {:.2f}%",
             total, valid, invalid, duplicates, violations, validationRate);
  }

  /**
   * Get default validation rules
   */
  private static List<PayrollQualityRule> getDefaultRules() {
    return Arrays.asList(
        PayrollQualityRule.createSSNValidationRule(),
        PayrollQualityRule.createEmailValidationRule(),
        PayrollQualityRule.createAgeRangeRule(),
        PayrollQualityRule.createWageComplianceRule()
    );
  }

  /**
   * Get validation statistics
   */
  public ValidationStatistics getStatistics() {
    return new ValidationStatistics(
        totalRecordsProcessed.get(),
        totalValidRecords.get(),
        totalInvalidRecords.get(),
        totalDuplicatesDetected.get(),
        totalComplianceViolations.get()
    );
  }

  /**
   * Validation statistics data class
   */
  public static class ValidationStatistics {
    private final long totalRecords;
    private final long validRecords;
    private final long invalidRecords;
    private final long duplicatesDetected;
    private final long complianceViolations;

    public ValidationStatistics(long totalRecords, long validRecords, long invalidRecords,
                               long duplicatesDetected, long complianceViolations) {
      this.totalRecords = totalRecords;
      this.validRecords = validRecords;
      this.invalidRecords = invalidRecords;
      this.duplicatesDetected = duplicatesDetected;
      this.complianceViolations = complianceViolations;
    }

    public long getTotalRecords() { return totalRecords; }
    public long getValidRecords() { return validRecords; }
    public long getInvalidRecords() { return invalidRecords; }
    public long getDuplicatesDetected() { return duplicatesDetected; }
    public long getComplianceViolations() { return complianceViolations; }

    public double getValidationSuccessRate() {
      return totalRecords > 0 ? (double) validRecords / totalRecords : 0.0;
    }

    @Override
    public String toString() {
      return String.format(
          "ValidationStatistics{total=%d, valid=%d, invalid=%d, duplicates=%d, violations=%d, successRate=%.2f%%}",
          totalRecords, validRecords, invalidRecords, duplicatesDetected, complianceViolations,
          getValidationSuccessRate() * 100);
    }
  }

  /**
   * Builder for PayrollValidationOperator configuration
   */
  public static class Builder {
    private List<PayrollQualityRule> rules = getDefaultRules();
    private boolean enableDuplicateDetection = true;
    private boolean enableComplianceAuditing = true;
    private boolean enablePIIEncryption = false;
    private Duration duplicateDetectionWindow = Duration.ofMinutes(60);

    public Builder withRules(List<PayrollQualityRule> rules) {
      this.rules = new ArrayList<>(rules);
      return this;
    }

    public Builder enableDuplicateDetection(boolean enable) {
      this.enableDuplicateDetection = enable;
      return this;
    }

    public Builder enableComplianceAuditing(boolean enable) {
      this.enableComplianceAuditing = enable;
      return this;
    }

    public Builder enablePIIEncryption(boolean enable) {
      this.enablePIIEncryption = enable;
      return this;
    }

    public Builder duplicateDetectionWindow(Duration window) {
      this.duplicateDetectionWindow = window;
      return this;
    }

    public PayrollValidationOperator build() {
      return new PayrollValidationOperator(rules, enableDuplicateDetection,
          enableComplianceAuditing, enablePIIEncryption, duplicateDetectionWindow);
    }
  }
}