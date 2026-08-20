package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import com.flinkpipeline.payroll.models.FieldValidationResult.FieldStatus;
import com.flinkpipeline.payroll.models.FieldValidationResult.RuleType;
import com.flinkpipeline.payroll.models.FieldValidationResult.Severity;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * SSN format validation rule implementation (DQ-005). Validates Social Security Numbers according
 * to federal format requirements. Includes blacklist checking for known invalid/test SSNs.
 */
public class SSNValidationRule {

  private static final String RULE_ID = "DQ-005";
  private static final String RULE_NAME = "SSN Format Validation";
  private static final String FIELD_NAME = "ssn";

  // SSN format pattern: XXX-XX-XXXX
  private static final Pattern SSN_PATTERN = Pattern.compile("^\\d{3}-\\d{2}-\\d{4}$");

  // Cache for blacklist lookups to improve performance
  private final ConcurrentHashMap<String, Boolean> blacklistCache = new ConcurrentHashMap<>();

  // Known invalid/test SSNs that should be blacklisted
  private static final Set<String> BLACKLISTED_SSNS =
      Set.of(
          "000-00-0000", // All zeros
          "123-45-6789", // Common test number
          "111-11-1111", // All same digit
          "222-22-2222", // All same digit
          "333-33-3333", // All same digit
          "444-44-4444", // All same digit
          "555-55-5555", // All same digit
          "666-66-6666", // All same digit (666 area is invalid)
          "777-77-7777", // All same digit
          "888-88-8888", // All same digit
          "999-99-9999", // All same digit
          "078-05-1120", // Woolworth's fake SSN
          "219-09-9999" // Death Master File test number
          );

  /** Validates SSN format and checks against blacklist */
  public FieldValidationResult validate(String ssn) {
    long startTime = System.nanoTime();

    try {
      // Check for null or empty SSN
      if (ssn == null || ssn.trim().isEmpty()) {
        return createFailureResult(
            "SSN is required and cannot be empty",
            "Enter SSN in format XXX-XX-XXXX (e.g., 123-45-6789)",
            "SSN_REQUIRED");
      }

      // Trim whitespace
      ssn = ssn.trim();

      // Validate format
      if (!SSN_PATTERN.matcher(ssn).matches()) {
        return createFailureResult(
            "Invalid SSN format - must be XXX-XX-XXXX",
            "Enter SSN in format XXX-XX-XXXX (e.g., 123-45-6789)",
            "SSN_FORMAT_INVALID");
      }

      // Check area number (first 3 digits)
      String areaNumber = ssn.substring(0, 3);
      if ("000".equals(areaNumber)
          || "666".equals(areaNumber)
          || (Integer.parseInt(areaNumber) >= 900)) {
        return createFailureResult(
            "SSN area number " + areaNumber + " is invalid",
            "Verify SSN is correct - area numbers 000, 666, and 900-999 are invalid",
            "SSN_AREA_INVALID");
      }

      // Check group number (middle 2 digits)
      String groupNumber = ssn.substring(4, 6);
      if ("00".equals(groupNumber)) {
        return createFailureResult(
            "SSN group number 00 is invalid",
            "Verify SSN is correct - group number cannot be 00",
            "SSN_GROUP_INVALID");
      }

      // Check serial number (last 4 digits)
      String serialNumber = ssn.substring(7);
      if ("0000".equals(serialNumber)) {
        return createFailureResult(
            "SSN serial number 0000 is invalid",
            "Verify SSN is correct - serial number cannot be 0000",
            "SSN_SERIAL_INVALID");
      }

      // Check against blacklist
      if (isBlacklistedSSN(ssn)) {
        return createFailureResult(
            "SSN " + ssn + " appears to be invalid or test data",
            "Verify SSN is real and not a test number (e.g., 000-00-0000, 123-45-6789)",
            "SSN_BLACKLISTED");
      }

      // SSN is valid
      return FieldValidationResult.builder()
          .fieldName(FIELD_NAME)
          .ruleName(RULE_NAME)
          .ruleType(RuleType.FORMAT)
          .status(FieldStatus.PASSED)
          .severity(Severity.INFO)
          .complianceLevel(ComplianceLevel.REGULATORY)
          .build();

    } finally {
      // Track performance
      long duration = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds
      if (duration > 50) {
        // Log performance issue if validation takes longer than 50ms SLA
        System.err.println("WARNING: SSN validation exceeded 50ms SLA: " + duration + "ms");
      }
    }
  }

  /** Validates SSN from string input with type conversion */
  public FieldValidationResult validateFromString(String ssnString) {
    return validate(ssnString);
  }

  /** Checks if SSN is in the blacklist (with caching for performance) */
  private boolean isBlacklistedSSN(String ssn) {
    return blacklistCache.computeIfAbsent(ssn, this::checkBlacklist);
  }

  /** Performs actual blacklist check */
  private boolean checkBlacklist(String ssn) {
    return BLACKLISTED_SSNS.contains(ssn);
  }

  /** Creates a failure validation result */
  private FieldValidationResult createFailureResult(
      String errorMessage, String suggestedCorrection, String errorCode) {
    return FieldValidationResult.builder()
        .fieldName(FIELD_NAME)
        .ruleName(RULE_NAME)
        .ruleType(RuleType.FORMAT)
        .status(FieldStatus.FAILED)
        .errorMessage(errorMessage)
        .severity(Severity.CRITICAL)
        .suggestedCorrection(suggestedCorrection)
        .errorCode(errorCode)
        .complianceLevel(ComplianceLevel.REGULATORY)
        .build();
  }

  /** Formats currency for display in error messages */
  public String formatSSN(String ssn) {
    if (ssn == null || ssn.length() != 11) {
      return ssn;
    }
    // Mask SSN for display: XXX-XX-1234
    return "***-**-" + ssn.substring(7);
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
    return RuleType.FORMAT;
  }

  public ComplianceLevel getComplianceLevel() {
    return ComplianceLevel.REGULATORY;
  }

  /** Clears the blacklist cache (for testing) */
  public void clearCache() {
    blacklistCache.clear();
  }

  /** Gets cache size (for monitoring) */
  public int getCacheSize() {
    return blacklistCache.size();
  }
}
