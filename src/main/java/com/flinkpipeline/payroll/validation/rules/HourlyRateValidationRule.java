package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import com.flinkpipeline.payroll.models.FieldValidationResult.FieldStatus;
import com.flinkpipeline.payroll.models.FieldValidationResult.RuleType;
import com.flinkpipeline.payroll.models.FieldValidationResult.Severity;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * Hourly rate wage compliance validation rule implementation (DQ-007).
 * Validates hourly wage rates against federal minimum wage and executive compensation limits.
 * Enforces federal employment law compliance for wage requirements.
 */
public class HourlyRateValidationRule {

  private static final String RULE_ID = "DQ-007";
  private static final String RULE_NAME = "Hourly Rate Range";
  private static final String FIELD_NAME = "hourly_rate";

  // Wage limits in cents for precision
  private static final int MINIMUM_WAGE_CENTS = 725;  // $7.25 federal minimum wage
  private static final int MAXIMUM_WAGE_CENTS = 15000; // $150.00 executive cap

  // Currency formatter for error messages
  private static final NumberFormat CURRENCY_FORMAT = NumberFormat.getCurrencyInstance(Locale.US);

  /**
   * Validates hourly rate against federal wage requirements
   */
  public FieldValidationResult validate(Integer hourlyRateCents) {
    long startTime = System.nanoTime();

    try {
      // Check for null rate
      if (hourlyRateCents == null) {
        return createFailureResult("Hourly rate is required and cannot be null",
            "Enter hourly rate between " + formatCurrency(MINIMUM_WAGE_CENTS) +
            " and " + formatCurrency(MAXIMUM_WAGE_CENTS),
            "WAGE_REQUIRED");
      }

      // Check for negative rate
      if (hourlyRateCents < 0) {
        return createFailureResult("Hourly rate " + formatCurrency(hourlyRateCents) + " is invalid - cannot be negative",
            "Enter a positive hourly rate between " + formatCurrency(MINIMUM_WAGE_CENTS) +
            " and " + formatCurrency(MAXIMUM_WAGE_CENTS),
            "WAGE_NEGATIVE");
      }

      // Check for zero rate
      if (hourlyRateCents == 0) {
        return createFailureResult("Hourly rate cannot be zero",
            "Enter hourly rate between " + formatCurrency(MINIMUM_WAGE_CENTS) +
            " and " + formatCurrency(MAXIMUM_WAGE_CENTS),
            "WAGE_ZERO");
      }

      // Check minimum wage requirement
      if (hourlyRateCents < MINIMUM_WAGE_CENTS) {
        return createFailureResult(
            "Hourly rate " + formatCurrency(hourlyRateCents) + " is outside valid range (" +
            formatCurrency(MINIMUM_WAGE_CENTS) + " - " + formatCurrency(MAXIMUM_WAGE_CENTS) + ")",
            "Verify hourly rate is within federal minimum wage (" + formatCurrency(MINIMUM_WAGE_CENTS) +
            ") and executive cap (" + formatCurrency(MAXIMUM_WAGE_CENTS) + ")",
            "WAGE_BELOW_MINIMUM");
      }

      // Check maximum wage cap
      if (hourlyRateCents > MAXIMUM_WAGE_CENTS) {
        return createFailureResult(
            "Hourly rate " + formatCurrency(hourlyRateCents) + " is outside valid range (" +
            formatCurrency(MINIMUM_WAGE_CENTS) + " - " + formatCurrency(MAXIMUM_WAGE_CENTS) + ")",
            "Verify hourly rate is within federal minimum wage (" + formatCurrency(MINIMUM_WAGE_CENTS) +
            ") and executive cap (" + formatCurrency(MAXIMUM_WAGE_CENTS) + ")",
            "WAGE_ABOVE_MAXIMUM");
      }

      // Hourly rate is valid
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
        System.err.println("WARNING: Wage validation exceeded 50ms SLA: " + duration + "ms");
      }
    }
  }

  /**
   * Validates hourly rate from dollars (converts to cents)
   */
  public FieldValidationResult validateFromDollars(Double hourlyRateDollars) {
    if (hourlyRateDollars == null) {
      return validate(null);
    }

    // Convert dollars to cents for precision
    int hourlyRateCents = (int) Math.round(hourlyRateDollars * 100);
    return validate(hourlyRateCents);
  }

  /**
   * Validates hourly rate from string input with type conversion
   */
  public FieldValidationResult validateFromString(String rateString) {
    if (rateString == null || rateString.trim().isEmpty()) {
      return createFailureResult("Hourly rate is required and cannot be empty",
          "Enter hourly rate between " + formatCurrency(MINIMUM_WAGE_CENTS) +
          " and " + formatCurrency(MAXIMUM_WAGE_CENTS),
          "WAGE_REQUIRED");
    }

    try {
      // Try to parse as dollars first
      String cleanRate = rateString.trim().replace("$", "").replace(",", "");
      Double rateDollars = Double.parseDouble(cleanRate);
      return validateFromDollars(rateDollars);
    } catch (NumberFormatException e) {
      return createFailureResult("Hourly rate '" + rateString + "' is not a valid number",
          "Enter a numeric hourly rate between " + formatCurrency(MINIMUM_WAGE_CENTS) +
          " and " + formatCurrency(MAXIMUM_WAGE_CENTS),
          "WAGE_FORMAT_INVALID");
    }
  }

  /**
   * Validates hourly rate from annual salary conversion
   */
  public FieldValidationResult validateFromAnnualSalary(Integer annualSalary) {
    if (annualSalary == null || annualSalary <= 0) {
      return createFailureResult("Annual salary must be positive for conversion",
          "Enter valid annual salary for hourly rate calculation",
          "SALARY_INVALID");
    }

    // Convert annual salary to hourly rate (assuming 52 weeks * 40 hours)
    double hourlyRateDollars = annualSalary / (52.0 * 40.0);
    int hourlyRateCents = (int) Math.round(hourlyRateDollars * 100);

    return validate(hourlyRateCents);
  }

  /**
   * Checks if rate meets federal minimum wage
   */
  public boolean meetsFederalMinimum(Integer hourlyRateCents) {
    return hourlyRateCents != null && hourlyRateCents >= MINIMUM_WAGE_CENTS;
  }

  /**
   * Checks if rate is within executive compensation cap
   */
  public boolean withinExecutiveCap(Integer hourlyRateCents) {
    return hourlyRateCents != null && hourlyRateCents <= MAXIMUM_WAGE_CENTS;
  }

  /**
   * Checks if rate is within full wage range
   */
  public boolean isValidWageRange(Integer hourlyRateCents) {
    return hourlyRateCents != null &&
           hourlyRateCents >= MINIMUM_WAGE_CENTS &&
           hourlyRateCents <= MAXIMUM_WAGE_CENTS;
  }

  /**
   * Formats currency for display in error messages
   */
  public String formatCurrency(Integer cents) {
    if (cents == null) return "$0.00";
    return CURRENCY_FORMAT.format(cents / 100.0);
  }

  /**
   * Creates a failure validation result
   */
  private FieldValidationResult createFailureResult(String errorMessage,
                                                   String suggestedCorrection,
                                                   String errorCode) {
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

  /**
   * Gets the rule configuration details
   */
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

  /**
   * Gets wage range limits
   */
  public int getMinimumWageCents() {
    return MINIMUM_WAGE_CENTS;
  }

  public int getMaximumWageCents() {
    return MAXIMUM_WAGE_CENTS;
  }

  public double getMinimumWageDollars() {
    return MINIMUM_WAGE_CENTS / 100.0;
  }

  public double getMaximumWageDollars() {
    return MAXIMUM_WAGE_CENTS / 100.0;
  }

  /**
   * Validates wage category against specific tiers
   */
  public FieldValidationResult validateWageCategory(Integer hourlyRateCents) {
    if (hourlyRateCents == null) {
      return validate(hourlyRateCents);
    }

    // Check for tipped minimum wage (should not be used for standard employees)
    if (hourlyRateCents <= 213) { // $2.13 tipped minimum
      return createFailureResult(
          "Hourly rate " + formatCurrency(hourlyRateCents) + " appears to be tipped minimum wage",
          "Use standard minimum wage for non-tipped employees",
          "TIPPED_WAGE_INVALID");
    }

    return validate(hourlyRateCents);
  }

  /**
   * Gets wage tier classification
   */
  public String getWageTier(Integer hourlyRateCents) {
    if (hourlyRateCents == null || hourlyRateCents < MINIMUM_WAGE_CENTS) {
      return "INVALID";
    } else if (hourlyRateCents <= 1000) { // Up to $10.00
      return "ENTRY_LEVEL";
    } else if (hourlyRateCents <= 2000) { // Up to $20.00
      return "SKILLED";
    } else if (hourlyRateCents <= 5000) { // Up to $50.00
      return "PROFESSIONAL";
    } else if (hourlyRateCents <= 10000) { // Up to $100.00
      return "SENIOR_PROFESSIONAL";
    } else {
      return "EXECUTIVE";
    }
  }
}