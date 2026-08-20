package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import com.flinkpipeline.payroll.models.FieldValidationResult.FieldStatus;
import com.flinkpipeline.payroll.models.FieldValidationResult.RuleType;
import com.flinkpipeline.payroll.models.FieldValidationResult.Severity;

/**
 * Age range employment eligibility validation rule implementation (DQ-004). Validates employee age
 * according to employment eligibility requirements (16-75 years). Enforces federal employment law
 * compliance.
 */
public class AgeRangeValidationRule {

  private static final String RULE_ID = "DQ-004";
  private static final String RULE_NAME = "Age Employment Eligibility";
  private static final String FIELD_NAME = "age";

  // Employment eligibility age range
  private static final int MINIMUM_EMPLOYMENT_AGE = 16;
  private static final int MAXIMUM_EMPLOYMENT_AGE = 75;

  /** Validates age against employment eligibility requirements */
  public FieldValidationResult validate(Integer age) {
    long startTime = System.nanoTime();

    try {
      // Check for null age
      if (age == null) {
        return createFailureResult(
            "Age is required and cannot be null",
            "Enter a valid age between "
                + MINIMUM_EMPLOYMENT_AGE
                + " and "
                + MAXIMUM_EMPLOYMENT_AGE
                + " years",
            "AGE_REQUIRED");
      }

      // Check for negative age
      if (age < 0) {
        return createFailureResult(
            "Age " + age + " is invalid - cannot be negative",
            "Enter a valid positive age between "
                + MINIMUM_EMPLOYMENT_AGE
                + " and "
                + MAXIMUM_EMPLOYMENT_AGE
                + " years",
            "AGE_NEGATIVE");
      }

      // Check for unrealistic age
      if (age > 150) {
        return createFailureResult(
            "Age " + age + " is unrealistic",
            "Verify age is correct and within reasonable range",
            "AGE_UNREALISTIC");
      }

      // Check minimum employment age
      if (age < MINIMUM_EMPLOYMENT_AGE) {
        return createFailureResult(
            "Age "
                + age
                + " is outside employment eligibility range ("
                + MINIMUM_EMPLOYMENT_AGE
                + "-"
                + MAXIMUM_EMPLOYMENT_AGE
                + " years)",
            "Verify employee age is correct and within legal employment range",
            "AGE_BELOW_MINIMUM");
      }

      // Check maximum employment age
      if (age > MAXIMUM_EMPLOYMENT_AGE) {
        return createFailureResult(
            "Age "
                + age
                + " is outside employment eligibility range ("
                + MINIMUM_EMPLOYMENT_AGE
                + "-"
                + MAXIMUM_EMPLOYMENT_AGE
                + " years)",
            "Verify employee age is correct and within legal employment range",
            "AGE_ABOVE_MAXIMUM");
      }

      // Age is valid
      return FieldValidationResult.builder()
          .fieldName(FIELD_NAME)
          .ruleName(RULE_NAME)
          .ruleType(RuleType.RANGE)
          .status(FieldStatus.PASSED)
          .severity(Severity.INFO)
          .complianceLevel(ComplianceLevel.REGULATORY)
          .build();

    } finally {
      // Track performance
      long duration = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds
      if (duration > 50) {
        // Log performance issue if validation takes longer than 50ms SLA
        System.err.println("WARNING: Age validation exceeded 50ms SLA: " + duration + "ms");
      }
    }
  }

  /** Validates age from string input with type conversion */
  public FieldValidationResult validateFromString(String ageString) {
    if (ageString == null || ageString.trim().isEmpty()) {
      return createFailureResult(
          "Age is required and cannot be empty",
          "Enter a valid age between "
              + MINIMUM_EMPLOYMENT_AGE
              + " and "
              + MAXIMUM_EMPLOYMENT_AGE
              + " years",
          "AGE_REQUIRED");
    }

    try {
      Integer age = Integer.parseInt(ageString.trim());
      return validate(age);
    } catch (NumberFormatException e) {
      return createFailureResult(
          "Age '" + ageString + "' is not a valid number",
          "Enter a numeric age between "
              + MINIMUM_EMPLOYMENT_AGE
              + " and "
              + MAXIMUM_EMPLOYMENT_AGE
              + " years",
          "AGE_FORMAT_INVALID");
    }
  }

  /** Checks if age meets minimum employment requirements */
  public boolean meetsMinimumAge(Integer age) {
    return age != null && age >= MINIMUM_EMPLOYMENT_AGE;
  }

  /** Checks if age is within maximum employment limit */
  public boolean withinMaximumAge(Integer age) {
    return age != null && age <= MAXIMUM_EMPLOYMENT_AGE;
  }

  /** Checks if age is within full employment eligibility range */
  public boolean isEmploymentEligible(Integer age) {
    return age != null && age >= MINIMUM_EMPLOYMENT_AGE && age <= MAXIMUM_EMPLOYMENT_AGE;
  }

  /** Creates a failure validation result */
  private FieldValidationResult createFailureResult(
      String errorMessage, String suggestedCorrection, String errorCode) {
    return FieldValidationResult.builder()
        .fieldName(FIELD_NAME)
        .ruleName(RULE_NAME)
        .ruleType(RuleType.RANGE)
        .status(FieldStatus.FAILED)
        .errorMessage(errorMessage)
        .severity(Severity.CRITICAL)
        .suggestedCorrection(suggestedCorrection)
        .errorCode(errorCode)
        .complianceLevel(ComplianceLevel.REGULATORY)
        .build();
  }

  /** Gets the rule configuration details */
  public String getRuleId() {
    return RULE_ID;
  }

  public String getRuleName() {
    return RULE_NAME;
  }

  public String getFieldName() {
    return FIELD_NAME;
  }

  public RuleType getRuleType() {
    return RuleType.RANGE;
  }

  public ComplianceLevel getComplianceLevel() {
    return ComplianceLevel.REGULATORY;
  }

  /** Gets employment age range */
  public int getMinimumEmploymentAge() {
    return MINIMUM_EMPLOYMENT_AGE;
  }

  public int getMaximumEmploymentAge() {
    return MAXIMUM_EMPLOYMENT_AGE;
  }

  /** Validates against specific employment law categories */
  public FieldValidationResult validateEmploymentCategory(Integer age) {
    if (age == null) {
      return validate(age);
    }

    if (age < 14) {
      return createFailureResult(
          "Age " + age + " violates child labor laws",
          "Employees under 14 are generally prohibited from most employment",
          "CHILD_LABOR_VIOLATION");
    }

    if (age >= 14 && age < 16) {
      return createFailureResult(
          "Age " + age + " requires special work permits and restrictions",
          "Employees 14-15 require work permits and have restricted hours",
          "MINOR_WORK_PERMIT_REQUIRED");
    }

    return validate(age);
  }
}
