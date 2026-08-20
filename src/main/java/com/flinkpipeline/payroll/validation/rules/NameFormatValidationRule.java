package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import java.util.regex.Pattern;

/**
 * Validation rule for employee name format compliance. Validates first and last names according to
 * business rules for character sets, length limits, and professional naming standards for payroll
 * systems.
 */
public class NameFormatValidationRule {

  // Name validation patterns
  private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z\\s\\-']{1,50}$");
  private static final Pattern PROFESSIONAL_NAME_PATTERN =
      Pattern.compile("^[a-zA-Z][a-zA-Z\\s\\-']*[a-zA-Z]$");
  private static final Pattern SUSPICIOUS_PATTERN =
      Pattern.compile(".*[0-9@#$%^&*()+={}\\[\\]|\\\\:;\"<>?,./].*");

  // Configuration
  private static final int MIN_NAME_LENGTH = 1;
  private static final int MAX_NAME_LENGTH = 50;
  private static final boolean ENFORCE_PROFESSIONAL_FORMAT = true;
  private static final boolean DETECT_SUSPICIOUS_PATTERNS = true;

  /** Validates first name format and compliance */
  public FieldValidationResult validateFirstName(String firstName) {
    return validateNameField(firstName, "first_name", "First Name Validation");
  }

  /** Validates last name format and compliance */
  public FieldValidationResult validateLastName(String lastName) {
    return validateNameField(lastName, "last_name", "Last Name Validation");
  }

  /** Generic name field validation */
  private FieldValidationResult validateNameField(String name, String fieldName, String ruleName) {
    // Null or empty check
    if (name == null) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name is required and cannot be null",
          "Enter a valid " + fieldName.replace("_", " "),
          ComplianceLevel.BUSINESS);
    }

    String trimmedName = name.trim();
    if (trimmedName.isEmpty()) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name is required and cannot be empty",
          "Enter a valid " + fieldName.replace("_", " "),
          ComplianceLevel.BUSINESS);
    }

    // Length validation
    if (trimmedName.length() < MIN_NAME_LENGTH) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name is too short (minimum " + MIN_NAME_LENGTH + " character)",
          "Enter a name with at least " + MIN_NAME_LENGTH + " character",
          ComplianceLevel.BUSINESS);
    }

    if (trimmedName.length() > MAX_NAME_LENGTH) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name is too long (maximum " + MAX_NAME_LENGTH + " characters)",
          "Shorten name to " + MAX_NAME_LENGTH + " characters or less",
          ComplianceLevel.BUSINESS);
    }

    // Basic character validation
    if (!VALID_NAME_PATTERN.matcher(trimmedName).matches()) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name contains invalid characters",
          "Use only letters, spaces, hyphens, and apostrophes",
          ComplianceLevel.BUSINESS);
    }

    // Suspicious pattern detection
    if (DETECT_SUSPICIOUS_PATTERNS && SUSPICIOUS_PATTERN.matcher(trimmedName).matches()) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name contains suspicious characters or patterns",
          "Remove numbers, symbols, or special characters from name",
          ComplianceLevel.BUSINESS);
    }

    // Professional format validation
    if (ENFORCE_PROFESSIONAL_FORMAT && !PROFESSIONAL_NAME_PATTERN.matcher(trimmedName).matches()) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name format does not meet professional standards",
          "Ensure name starts and ends with letters, contains only valid characters",
          ComplianceLevel.BUSINESS);
    }

    // Additional business rule validations
    FieldValidationResult businessRuleResult =
        validateBusinessRules(trimmedName, fieldName, ruleName);
    if (businessRuleResult.isFailed()) {
      return businessRuleResult;
    }

    // All validations passed
    return FieldValidationResult.success(fieldName, ruleName);
  }

  /** Additional business rule validations for names */
  private FieldValidationResult validateBusinessRules(
      String name, String fieldName, String ruleName) {
    // Check for repeated characters (e.g., "aaaa")
    if (hasExcessiveRepeatedCharacters(name)) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name contains excessive repeated characters",
          "Review name for typing errors or unusual patterns",
          ComplianceLevel.BUSINESS);
    }

    // Check for common test/placeholder names
    if (isTestOrPlaceholderName(name)) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name appears to be a test or placeholder value",
          "Enter the actual employee name instead of test data",
          ComplianceLevel.BUSINESS);
    }

    // Check for single character names (except valid cases)
    if (name.length() == 1 && !isValidSingleCharacterName(name)) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Single character names require validation",
          "Verify that single character name is correct and complete",
          ComplianceLevel.BUSINESS);
    }

    // Check for excessive spaces
    if (hasExcessiveSpaces(name)) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name contains excessive spaces",
          "Remove extra spaces between name parts",
          ComplianceLevel.BUSINESS);
    }

    // Check for leading/trailing hyphens or apostrophes
    if (hasInvalidPunctuation(name)) {
      return FieldValidationResult.failure(
          fieldName,
          ruleName,
          "Name has invalid punctuation placement",
          "Remove hyphens or apostrophes from beginning or end of name",
          ComplianceLevel.BUSINESS);
    }

    return FieldValidationResult.success(fieldName, ruleName);
  }

  /** Check for excessive repeated characters */
  private boolean hasExcessiveRepeatedCharacters(String name) {
    if (name.length() < 3) return false;

    int maxRepeated = 0;
    int currentRepeated = 1;
    char previousChar = name.charAt(0);

    for (int i = 1; i < name.length(); i++) {
      char currentChar = name.charAt(i);
      if (Character.toLowerCase(currentChar) == Character.toLowerCase(previousChar)) {
        currentRepeated++;
        maxRepeated = Math.max(maxRepeated, currentRepeated);
      } else {
        currentRepeated = 1;
      }
      previousChar = currentChar;
    }

    return maxRepeated > 3; // More than 3 consecutive identical characters
  }

  /** Check for common test or placeholder names */
  private boolean isTestOrPlaceholderName(String name) {
    String lowerName = name.toLowerCase().replaceAll("\\s+", "");

    String[] testPatterns = {
      "test",
      "testing",
      "example",
      "sample",
      "demo",
      "placeholder",
      "temp",
      "temporary",
      "xxx",
      "yyy",
      "zzz",
      "firstname",
      "lastname",
      "name",
      "employee",
      "john",
      "jane",
      "foo",
      "bar",
      "baz"
    };

    for (String pattern : testPatterns) {
      if (lowerName.equals(pattern)
          || lowerName.startsWith(pattern + "1")
          || lowerName.startsWith(pattern + "2")
          || lowerName.startsWith(pattern + "3")) {
        return true;
      }
    }

    return false;
  }

  /** Check if single character name is valid (e.g., some cultures use single character names) */
  private boolean isValidSingleCharacterName(String name) {
    // Allow single uppercase letters as they might be valid in some cultures
    return name.length() == 1 && Character.isUpperCase(name.charAt(0));
  }

  /** Check for excessive spaces in name */
  private boolean hasExcessiveSpaces(String name) {
    // Check for multiple consecutive spaces
    if (name.contains("  ")) { // Two or more spaces
      return true;
    }

    // Check for excessive number of spaces (more than 3 spaces total suggests multiple middle
    // names)
    long spaceCount = name.chars().filter(ch -> ch == ' ').count();
    return spaceCount > 3;
  }

  /** Check for invalid punctuation placement */
  private boolean hasInvalidPunctuation(String name) {
    // Check for leading punctuation
    if (name.startsWith("-") || name.startsWith("'")) {
      return true;
    }

    // Check for trailing punctuation
    if (name.endsWith("-") || name.endsWith("'")) {
      return true;
    }

    // Check for consecutive punctuation
    if (name.contains("--") || name.contains("''") || name.contains("-'") || name.contains("'-")) {
      return true;
    }

    return false;
  }

  /** Validate full name (first + last) combination */
  public FieldValidationResult validateFullName(String firstName, String lastName) {
    if (firstName == null && lastName == null) {
      return FieldValidationResult.failure(
          "full_name",
          "Full Name Validation",
          "Both first name and last name cannot be null",
          "Enter valid first and last names",
          ComplianceLevel.BUSINESS);
    }

    // Check if names are identical (suspicious)
    if (firstName != null
        && lastName != null
        && firstName.trim().equalsIgnoreCase(lastName.trim())
        && !firstName.trim().isEmpty()) {
      return FieldValidationResult.failure(
          "full_name",
          "Full Name Validation",
          "First name and last name are identical",
          "Verify that first and last names are entered correctly",
          ComplianceLevel.BUSINESS);
    }

    // Check total name length
    int totalLength =
        (firstName != null ? firstName.trim().length() : 0)
            + (lastName != null ? lastName.trim().length() : 0);

    if (totalLength > 100) { // Combined length limit
      return FieldValidationResult.failure(
          "full_name",
          "Full Name Validation",
          "Combined first and last name length exceeds maximum (100 characters)",
          "Shorten names or use abbreviations to fit within limit",
          ComplianceLevel.BUSINESS);
    }

    return FieldValidationResult.success("full_name", "Full Name Validation");
  }

  /** Get name formatting suggestions */
  public String getNameFormattingSuggestion(String name) {
    if (name == null || name.trim().isEmpty()) {
      return "Enter a valid name";
    }

    StringBuilder suggestion = new StringBuilder();
    String trimmed = name.trim();

    // Suggest proper capitalization
    if (!isProperlyCapitalized(trimmed)) {
      suggestion.append("Use proper capitalization (first letter uppercase, rest lowercase). ");
    }

    // Suggest removing extra spaces
    if (trimmed.contains("  ")) {
      suggestion.append("Remove extra spaces between words. ");
    }

    // Suggest fixing punctuation
    if (hasInvalidPunctuation(trimmed)) {
      suggestion.append("Remove hyphens or apostrophes from beginning/end. ");
    }

    if (suggestion.length() == 0) {
      return "Name format is acceptable";
    }

    return suggestion.toString().trim();
  }

  /** Check if name is properly capitalized */
  private boolean isProperlyCapitalized(String name) {
    if (name.isEmpty()) return false;

    // Split on spaces, hyphens, and apostrophes
    String[] parts = name.split("[\\s\\-']+");

    for (String part : parts) {
      if (part.isEmpty()) continue;

      // First character should be uppercase
      if (!Character.isUpperCase(part.charAt(0))) {
        return false;
      }

      // Remaining characters should be lowercase
      for (int i = 1; i < part.length(); i++) {
        if (Character.isUpperCase(part.charAt(i))) {
          return false;
        }
      }
    }

    return true;
  }

  /** Format name according to business standards */
  public String formatName(String name) {
    if (name == null) return null;

    String trimmed = name.trim();
    if (trimmed.isEmpty()) return trimmed;

    // Remove excessive spaces
    String cleaned = trimmed.replaceAll("\\s+", " ");

    // Proper case formatting
    StringBuilder formatted = new StringBuilder();
    boolean capitalizeNext = true;

    for (char c : cleaned.toCharArray()) {
      if (Character.isLetter(c)) {
        if (capitalizeNext) {
          formatted.append(Character.toUpperCase(c));
          capitalizeNext = false;
        } else {
          formatted.append(Character.toLowerCase(c));
        }
      } else {
        formatted.append(c);
        if (c == ' ' || c == '-' || c == '\'') {
          capitalizeNext = true;
        }
      }
    }

    return formatted.toString();
  }
}
