package com.flinkpipeline.payroll.validation;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import com.flinkpipeline.payroll.models.PayrollValidationResult;
import com.flinkpipeline.payroll.models.PayrollValidationResult.ValidationStatus;
import com.flinkpipeline.payroll.validation.rules.AgeRangeValidationRule;
import com.flinkpipeline.payroll.validation.rules.HourlyRateValidationRule;
import com.flinkpipeline.payroll.validation.rules.SSNValidationRule;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Payroll rule engine that orchestrates validation of employee records.
 * Executes multiple validation rules and aggregates results.
 * Supports parallel execution for performance optimization.
 */
public class PayrollRuleEngine {

  private static final String RULE_VERSION = "1.0.0";

  // Validation rule instances
  private final SSNValidationRule ssnValidationRule;
  private final AgeRangeValidationRule ageValidationRule;
  private final HourlyRateValidationRule hourlyRateValidationRule;

  // Executor for parallel rule execution
  private final ExecutorService executorService;
  private final boolean parallelExecution;

  public PayrollRuleEngine() {
    this(true); // Default to parallel execution
  }

  public PayrollRuleEngine(boolean parallelExecution) {
    this.ssnValidationRule = new SSNValidationRule();
    this.ageValidationRule = new AgeRangeValidationRule();
    this.hourlyRateValidationRule = new HourlyRateValidationRule();
    this.parallelExecution = parallelExecution;
    this.executorService = parallelExecution ?
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()) : null;
  }

  /**
   * Validates a complete payroll employee record
   */
  public PayrollValidationResult validate(PayrollEmployee employee) {
    long startTime = System.nanoTime();

    try {
      PayrollValidationResult result = new PayrollValidationResult(employee.getEmployeeId());
      result.setRuleVersion(RULE_VERSION);

      List<FieldValidationResult> fieldResults = new ArrayList<>();

      if (parallelExecution) {
        fieldResults = validateInParallel(employee);
      } else {
        fieldResults = validateSequentially(employee);
      }

      result.setFieldResults(fieldResults);
      result.updateOverallStatus();

      // Calculate processing latency
      long processingTime = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds
      result.setProcessingLatencyMs(processingTime);

      // Add compliance flags if needed
      addComplianceFlags(result, fieldResults);

      return result;

    } catch (Exception e) {
      // Handle validation errors
      PayrollValidationResult errorResult = new PayrollValidationResult(employee.getEmployeeId());
      errorResult.setOverallStatus(ValidationStatus.INVALID);
      errorResult.setRuleVersion(RULE_VERSION);

      FieldValidationResult errorField = FieldValidationResult.failure(
          "validation_engine",
          "Payroll Rule Engine",
          "Validation engine error: " + e.getMessage(),
          "Contact system administrator");

      errorResult.addFieldResult(errorField);
      return errorResult;
    }
  }

  /**
   * Validates all fields in parallel for better performance
   */
  private List<FieldValidationResult> validateInParallel(PayrollEmployee employee) {
    List<CompletableFuture<FieldValidationResult>> futures = new ArrayList<>();

    // SSN validation
    if (employee.getSsn() != null) {
      futures.add(CompletableFuture.supplyAsync(() ->
          ssnValidationRule.validate(employee.getSsn()), executorService));
    }

    // Age validation
    if (employee.getAge() != null) {
      futures.add(CompletableFuture.supplyAsync(() ->
          ageValidationRule.validate(employee.getAge()), executorService));
    }

    // Hourly rate validation
    if (employee.getHourlyRate() != null) {
      futures.add(CompletableFuture.supplyAsync(() ->
          hourlyRateValidationRule.validate(employee.getHourlyRate()), executorService));
    }

    // Basic field validations (run in current thread)
    List<FieldValidationResult> results = new ArrayList<>();
    results.addAll(validateBasicFields(employee));

    // Wait for parallel validations to complete
    for (CompletableFuture<FieldValidationResult> future : futures) {
      try {
        results.add(future.get());
      } catch (Exception e) {
        // Add error result for failed validation
        results.add(FieldValidationResult.failure(
            "unknown_field",
            "Parallel Validation",
            "Parallel validation failed: " + e.getMessage(),
            "Retry validation or contact administrator"));
      }
    }

    return results;
  }

  /**
   * Validates all fields sequentially
   */
  private List<FieldValidationResult> validateSequentially(PayrollEmployee employee) {
    List<FieldValidationResult> results = new ArrayList<>();

    // Basic field validations
    results.addAll(validateBasicFields(employee));

    // SSN validation
    if (employee.getSsn() != null) {
      results.add(ssnValidationRule.validate(employee.getSsn()));
    }

    // Age validation
    if (employee.getAge() != null) {
      results.add(ageValidationRule.validate(employee.getAge()));
    }

    // Hourly rate validation
    if (employee.getHourlyRate() != null) {
      results.add(hourlyRateValidationRule.validate(employee.getHourlyRate()));
    }

    return results;
  }

  /**
   * Validates basic required fields
   */
  private List<FieldValidationResult> validateBasicFields(PayrollEmployee employee) {
    List<FieldValidationResult> results = new ArrayList<>();

    // Employee ID validation
    if (employee.getEmployeeId() == null || employee.getEmployeeId() <= 0) {
      results.add(FieldValidationResult.failure(
          "employee_id",
          "Employee ID Validation",
          "Employee ID must be a positive integer",
          "Enter a valid positive employee ID"));
    } else {
      results.add(FieldValidationResult.success("employee_id", "Employee ID Validation"));
    }

    // First name validation
    if (employee.getFirstName() == null || employee.getFirstName().trim().isEmpty()) {
      results.add(FieldValidationResult.failure(
          "first_name",
          "First Name Validation",
          "First name is required and cannot be empty",
          "Enter employee's first name"));
    } else if (!employee.getFirstName().matches("^[a-zA-Z\\s\\-']{1,50}$")) {
      results.add(FieldValidationResult.failure(
          "first_name",
          "First Name Format",
          "First name contains invalid characters or exceeds length limit",
          "Use only letters, spaces, hyphens, and apostrophes (1-50 characters)"));
    } else {
      results.add(FieldValidationResult.success("first_name", "First Name Validation"));
    }

    // Last name validation
    if (employee.getLastName() == null || employee.getLastName().trim().isEmpty()) {
      results.add(FieldValidationResult.failure(
          "last_name",
          "Last Name Validation",
          "Last name is required and cannot be empty",
          "Enter employee's last name"));
    } else if (!employee.getLastName().matches("^[a-zA-Z\\s\\-']{1,50}$")) {
      results.add(FieldValidationResult.failure(
          "last_name",
          "Last Name Format",
          "Last name contains invalid characters or exceeds length limit",
          "Use only letters, spaces, hyphens, and apostrophes (1-50 characters)"));
    } else {
      results.add(FieldValidationResult.success("last_name", "Last Name Validation"));
    }

    // Gender validation
    if (employee.getGender() == null || employee.getGender().trim().isEmpty()) {
      results.add(FieldValidationResult.failure(
          "gender",
          "Gender Validation",
          "Gender is required",
          "Select from: male, female, non-binary, prefer-not-to-say"));
    } else if (!isValidGender(employee.getGender())) {
      results.add(FieldValidationResult.failure(
          "gender",
          "Gender Value Validation",
          "Gender '" + employee.getGender() + "' is not from approved list",
          "Select from: male, female, non-binary, prefer-not-to-say"));
    } else {
      results.add(FieldValidationResult.success("gender", "Gender Validation"));
    }

    // Email validation
    if (employee.getEmail() == null || employee.getEmail().trim().isEmpty()) {
      results.add(FieldValidationResult.failure(
          "email",
          "Email Validation",
          "Email is required",
          "Enter valid company email address"));
    } else if (!isValidEmail(employee.getEmail())) {
      results.add(FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Invalid email format or unauthorized domain",
          "Use valid email format with approved company domain"));
    } else {
      results.add(FieldValidationResult.success("email", "Email Validation"));
    }

    return results;
  }

  /**
   * Validates gender against approved values
   */
  private boolean isValidGender(String gender) {
    if (gender == null) return false;
    return List.of("male", "female", "non-binary", "prefer-not-to-say")
        .contains(gender.toLowerCase().trim());
  }

  /**
   * Validates email format and domain
   */
  private boolean isValidEmail(String email) {
    if (email == null) return false;
    String emailPattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$";
    return email.matches(emailPattern) && isApprovedDomain(email);
  }

  /**
   * Checks if email domain is approved
   */
  private boolean isApprovedDomain(String email) {
    // For now, allow common business domains
    // In production, this would check against a configurable list
    String domain = email.substring(email.indexOf('@') + 1).toLowerCase();
    return domain.endsWith(".com") || domain.endsWith(".org") || domain.endsWith(".net");
  }

  /**
   * Adds compliance flags based on validation results
   */
  private void addComplianceFlags(PayrollValidationResult result, List<FieldValidationResult> fieldResults) {
    boolean hasRegulatoryViolation = fieldResults.stream()
        .anyMatch(field -> field.isFailed() &&
            field.getComplianceLevel() == FieldValidationResult.ComplianceLevel.REGULATORY);

    if (hasRegulatoryViolation) {
      result.addComplianceFlag(PayrollValidationResult.ComplianceFlag.REGULATORY_VIOLATION);
      result.addComplianceFlag(PayrollValidationResult.ComplianceFlag.AUDIT_REQUIRED);
    }

    // Check for PII fields that need encryption
    boolean hasPIIFields = fieldResults.stream()
        .anyMatch(field -> "ssn".equals(field.getFieldName()) || "email".equals(field.getFieldName()));

    if (hasPIIFields) {
      result.addComplianceFlag(PayrollValidationResult.ComplianceFlag.PII_ENCRYPTION_REQUIRED);
    }
  }

  /**
   * Validates SSN specifically
   */
  public FieldValidationResult validateSSN(String ssn) {
    return ssnValidationRule.validate(ssn);
  }

  /**
   * Validates age specifically
   */
  public FieldValidationResult validateAge(Integer age) {
    return ageValidationRule.validate(age);
  }

  /**
   * Validates hourly rate specifically
   */
  public FieldValidationResult validateHourlyRate(Integer hourlyRateCents) {
    return hourlyRateValidationRule.validate(hourlyRateCents);
  }

  /**
   * Validates email specifically
   */
  public FieldValidationResult validateEmail(String email) {
    if (isValidEmail(email)) {
      return FieldValidationResult.success("email", "Email Format Validation");
    } else {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Invalid email format or unauthorized domain",
          "Use valid email format with approved company domain");
    }
  }

  /**
   * Validates gender specifically
   */
  public FieldValidationResult validateGender(String gender) {
    if (isValidGender(gender)) {
      return FieldValidationResult.success("gender", "Gender Value Validation");
    } else {
      return FieldValidationResult.failure(
          "gender",
          "Gender Value Validation",
          "Gender '" + gender + "' is not from approved list",
          "Select from: male, female, non-binary, prefer-not-to-say");
    }
  }

  /**
   * Gets the current rule version
   */
  public String getCurrentVersion() {
    return RULE_VERSION;
  }

  /**
   * Shuts down the executor service
   */
  public void shutdown() {
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}