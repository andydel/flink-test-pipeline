package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validation rule for employee email format and domain compliance.
 * Validates email addresses according to RFC standards, business domain policies,
 * and security requirements for corporate payroll systems.
 */
public class EmailValidationRule {

  // Email validation patterns
  private static final Pattern BASIC_EMAIL_PATTERN = Pattern.compile(
      "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
  );

  private static final Pattern STRICT_EMAIL_PATTERN = Pattern.compile(
      "^[a-zA-Z0-9][a-zA-Z0-9._%+-]*[a-zA-Z0-9]@[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9]\\.[a-zA-Z]{2,}$"
  );

  private static final Pattern SUSPICIOUS_PATTERN = Pattern.compile(
      ".*[<>\"'\\\\;\\[\\]{}|`~!#$%^&*()=+/?].*"
  );

  // Domain validation
  private static final Set<String> APPROVED_DOMAINS = new HashSet<>(Arrays.asList(
      "company.com", "corp.company.com", "internal.company.com",
      "gmail.com", "outlook.com", "hotmail.com", "yahoo.com", // Common external domains for contractors
      "consultant.com", "contractor.com" // Example contractor domains
  ));

  private static final Set<String> BLOCKED_DOMAINS = new HashSet<>(Arrays.asList(
      "tempmail.org", "10minutemail.com", "guerrillamail.com", "mailinator.com",
      "throwaway.email", "temp-mail.org", "fake.com", "test.com", "example.com"
  ));

  private static final Set<String> SUSPICIOUS_DOMAINS = new HashSet<>(Arrays.asList(
      "suspicious.com", "phishing.com", "malware.org", "spam.net"
  ));

  // Configuration
  private static final int MAX_EMAIL_LENGTH = 254; // RFC 5321 limit
  private static final int MAX_LOCAL_PART_LENGTH = 64; // RFC 5321 limit
  private static final boolean ENFORCE_CORPORATE_DOMAIN = false; // Set to true for strict corporate policy
  private static final boolean ALLOW_PLUS_ADDRESSING = true; // Allow email+tag@domain.com
  private static final boolean STRICT_VALIDATION = true; // Use strict pattern matching

  /**
   * Validates email address format and compliance
   */
  public FieldValidationResult validateEmail(String email) {
    // Null or empty check
    if (email == null) {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Email address is required",
          "Enter a valid company email address",
          ComplianceLevel.BUSINESS
      );
    }

    String trimmedEmail = email.trim();
    if (trimmedEmail.isEmpty()) {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Email address is required and cannot be empty",
          "Enter a valid company email address",
          ComplianceLevel.BUSINESS
      );
    }

    // Length validation
    if (trimmedEmail.length() > MAX_EMAIL_LENGTH) {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Email address is too long (maximum " + MAX_EMAIL_LENGTH + " characters)",
          "Use a shorter email address",
          ComplianceLevel.BUSINESS
      );
    }

    // Basic format validation
    Pattern emailPattern = STRICT_VALIDATION ? STRICT_EMAIL_PATTERN : BASIC_EMAIL_PATTERN;
    if (!emailPattern.matcher(trimmedEmail).matches()) {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Email address format is invalid",
          "Use valid email format: username@domain.com",
          ComplianceLevel.BUSINESS
      );
    }

    // Suspicious character detection
    if (SUSPICIOUS_PATTERN.matcher(trimmedEmail).matches()) {
      return FieldValidationResult.failure(
          "email",
          "Email Security Validation",
          "Email address contains suspicious characters",
          "Remove special characters and symbols from email address",
          ComplianceLevel.REGULATORY
      );
    }

    // Extract and validate parts
    String[] parts = trimmedEmail.split("@");
    if (parts.length != 2) {
      return FieldValidationResult.failure(
          "email",
          "Email Format Validation",
          "Email address must contain exactly one @ symbol",
          "Correct email format: username@domain.com",
          ComplianceLevel.BUSINESS
      );
    }

    String localPart = parts[0];
    String domain = parts[1].toLowerCase();

    // Local part validation
    FieldValidationResult localPartResult = validateLocalPart(localPart);
    if (localPartResult.isFailed()) {
      return localPartResult;
    }

    // Domain validation
    FieldValidationResult domainResult = validateDomain(domain);
    if (domainResult.isFailed()) {
      return domainResult;
    }

    // Business rule validation
    FieldValidationResult businessRuleResult = validateBusinessRules(trimmedEmail, localPart, domain);
    if (businessRuleResult.isFailed()) {
      return businessRuleResult;
    }

    // All validations passed
    return FieldValidationResult.success("email", "Email Format Validation");
  }

  /**
   * Validate local part (username) of email address
   */
  private FieldValidationResult validateLocalPart(String localPart) {
    if (localPart.isEmpty()) {
      return FieldValidationResult.failure(
          "email",
          "Email Local Part Validation",
          "Email username part cannot be empty",
          "Enter valid username before @ symbol",
          ComplianceLevel.BUSINESS
      );
    }

    if (localPart.length() > MAX_LOCAL_PART_LENGTH) {
      return FieldValidationResult.failure(
          "email",
          "Email Local Part Validation",
          "Email username part is too long (maximum " + MAX_LOCAL_PART_LENGTH + " characters)",
          "Use shorter username in email address",
          ComplianceLevel.BUSINESS
      );
    }

    // Check for invalid characters or patterns in local part
    if (localPart.startsWith(".") || localPart.endsWith(".")) {
      return FieldValidationResult.failure(
          "email",
          "Email Local Part Validation",
          "Email username cannot start or end with a period",
          "Remove periods from beginning or end of username",
          ComplianceLevel.BUSINESS
      );
    }

    if (localPart.contains("..")) {
      return FieldValidationResult.failure(
          "email",
          "Email Local Part Validation",
          "Email username cannot contain consecutive periods",
          "Remove double periods from username",
          ComplianceLevel.BUSINESS
      );
    }

    // Plus addressing validation
    if (!ALLOW_PLUS_ADDRESSING && localPart.contains("+")) {
      return FieldValidationResult.failure(
          "email",
          "Email Local Part Validation",
          "Plus addressing (+) is not allowed in email addresses",
          "Use standard email format without + symbols",
          ComplianceLevel.BUSINESS
      );
    }

    return FieldValidationResult.success("email", "Email Local Part Validation");
  }

  /**
   * Validate domain part of email address
   */
  private FieldValidationResult validateDomain(String domain) {
    if (domain.isEmpty()) {
      return FieldValidationResult.failure(
          "email",
          "Email Domain Validation",
          "Email domain cannot be empty",
          "Enter valid domain after @ symbol",
          ComplianceLevel.BUSINESS
      );
    }

    // Check blocked domains
    if (BLOCKED_DOMAINS.contains(domain)) {
      return FieldValidationResult.failure(
          "email",
          "Email Domain Validation",
          "Email domain is not allowed (temporary/disposable email)",
          "Use a permanent business email address",
          ComplianceLevel.REGULATORY
      );
    }

    // Check suspicious domains
    if (SUSPICIOUS_DOMAINS.contains(domain)) {
      return FieldValidationResult.failure(
          "email",
          "Email Security Validation",
          "Email domain is flagged as suspicious",
          "Use a trusted business email domain",
          ComplianceLevel.REGULATORY
      );
    }

    // Corporate domain enforcement
    if (ENFORCE_CORPORATE_DOMAIN && !isApprovedCorporateDomain(domain)) {
      return FieldValidationResult.failure(
          "email",
          "Email Domain Policy Validation",
          "Email must use approved corporate domain",
          "Use company email address with approved domain",
          ComplianceLevel.BUSINESS
      );
    }

    // Domain format validation
    if (!isValidDomainFormat(domain)) {
      return FieldValidationResult.failure(
          "email",
          "Email Domain Validation",
          "Email domain format is invalid",
          "Use valid domain format: domain.com",
          ComplianceLevel.BUSINESS
      );
    }

    return FieldValidationResult.success("email", "Email Domain Validation");
  }

  /**
   * Additional business rule validations
   */
  private FieldValidationResult validateBusinessRules(String email, String localPart, String domain) {
    // Check for role-based addresses that might not be appropriate for payroll
    if (isRoleBasedAddress(localPart)) {
      return FieldValidationResult.failure(
          "email",
          "Email Business Rule Validation",
          "Role-based email addresses should not be used for payroll",
          "Use personal email address instead of role-based address",
          ComplianceLevel.BUSINESS
      );
    }

    // Check for test or placeholder addresses
    if (isTestOrPlaceholderEmail(email)) {
      return FieldValidationResult.failure(
          "email",
          "Email Business Rule Validation",
          "Test or placeholder email addresses are not allowed",
          "Enter actual employee email address",
          ComplianceLevel.BUSINESS
      );
    }

    // Check for common typos in domain
    String domainSuggestion = suggestDomainCorrection(domain);
    if (domainSuggestion != null && !domainSuggestion.equals(domain)) {
      return FieldValidationResult.failure(
          "email",
          "Email Domain Validation",
          "Possible typo in email domain - did you mean: " + domainSuggestion + "?",
          "Check domain spelling and use: " + domainSuggestion,
          ComplianceLevel.BUSINESS
      );
    }

    return FieldValidationResult.success("email", "Email Business Rule Validation");
  }

  /**
   * Check if domain is approved corporate domain
   */
  private boolean isApprovedCorporateDomain(String domain) {
    return APPROVED_DOMAINS.contains(domain) ||
           domain.endsWith(".company.com") || // Allow subdomains
           domain.equals("company.com");
  }

  /**
   * Validate domain format
   */
  private boolean isValidDomainFormat(String domain) {
    // Basic domain format check
    if (domain.startsWith(".") || domain.endsWith(".") ||
        domain.startsWith("-") || domain.endsWith("-")) {
      return false;
    }

    if (domain.contains("..") || domain.contains("--")) {
      return false;
    }

    // Must contain at least one dot
    if (!domain.contains(".")) {
      return false;
    }

    // Check each part of domain
    String[] parts = domain.split("\\.");
    for (String part : parts) {
      if (part.isEmpty() || part.length() > 63) { // DNS label limit
        return false;
      }
      if (!part.matches("^[a-zA-Z0-9-]+$")) {
        return false;
      }
    }

    return true;
  }

  /**
   * Check for role-based email addresses
   */
  private boolean isRoleBasedAddress(String localPart) {
    String lowerLocal = localPart.toLowerCase();
    String[] rolePrefixes = {
        "admin", "administrator", "support", "help", "info", "sales",
        "marketing", "hr", "humanresources", "payroll", "accounting",
        "finance", "it", "technical", "webmaster", "noreply", "no-reply"
    };

    for (String role : rolePrefixes) {
      if (lowerLocal.equals(role) || lowerLocal.startsWith(role + ".") ||
          lowerLocal.startsWith(role + "+") || lowerLocal.startsWith(role + "-")) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check for test or placeholder email addresses
   */
  private boolean isTestOrPlaceholderEmail(String email) {
    String lowerEmail = email.toLowerCase();
    String[] testPatterns = {
        "test@", "testing@", "example@", "sample@", "demo@",
        "placeholder@", "temp@", "temporary@", "fake@",
        "@test.", "@testing.", "@example.", "@sample.", "@demo."
    };

    for (String pattern : testPatterns) {
      if (lowerEmail.contains(pattern)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Suggest domain correction for common typos
   */
  private String suggestDomainCorrection(String domain) {
    // Common domain typos and their corrections
    String[][] commonTypos = {
        {"gmial.com", "gmail.com"},
        {"gmai.com", "gmail.com"},
        {"gmail.co", "gmail.com"},
        {"hotmial.com", "hotmail.com"},
        {"hotmai.com", "hotmail.com"},
        {"yahooo.com", "yahoo.com"},
        {"yaho.com", "yahoo.com"},
        {"outlook.co", "outlook.com"},
        {"outlok.com", "outlook.com"},
        {"company.co", "company.com"},
        {"compnay.com", "company.com"}
    };

    for (String[] typo : commonTypos) {
      if (domain.equals(typo[0])) {
        return typo[1];
      }
    }

    return null; // No suggestion
  }

  /**
   * Get email formatting suggestions
   */
  public String getEmailFormattingSuggestion(String email) {
    if (email == null || email.trim().isEmpty()) {
      return "Enter a valid email address in format: username@domain.com";
    }

    StringBuilder suggestion = new StringBuilder();
    String trimmed = email.trim();

    // Check for basic format issues
    if (!trimmed.contains("@")) {
      suggestion.append("Add @ symbol between username and domain. ");
    } else if (trimmed.split("@").length != 2) {
      suggestion.append("Use exactly one @ symbol. ");
    }

    // Check for common formatting issues
    if (trimmed.contains(" ")) {
      suggestion.append("Remove spaces from email address. ");
    }

    if (trimmed.startsWith(".") || trimmed.endsWith(".")) {
      suggestion.append("Remove periods from beginning or end. ");
    }

    // Domain suggestions
    if (trimmed.contains("@")) {
      String domain = trimmed.split("@")[1].toLowerCase();
      String domainSuggestion = suggestDomainCorrection(domain);
      if (domainSuggestion != null) {
        suggestion.append("Check domain spelling - did you mean: ").append(domainSuggestion).append("? ");
      }
    }

    if (suggestion.length() == 0) {
      return "Email format appears acceptable";
    }

    return suggestion.toString().trim();
  }

  /**
   * Format email according to business standards
   */
  public String formatEmail(String email) {
    if (email == null) return null;

    String trimmed = email.trim();
    if (trimmed.isEmpty()) return trimmed;

    // Convert to lowercase (standard practice)
    String formatted = trimmed.toLowerCase();

    // Remove any spaces
    formatted = formatted.replaceAll("\\s+", "");

    return formatted;
  }

  /**
   * Check if email domain is external (not corporate)
   */
  public boolean isExternalDomain(String email) {
    if (email == null || !email.contains("@")) {
      return true; // Assume external if invalid
    }

    String domain = email.split("@")[1].toLowerCase();
    return !isApprovedCorporateDomain(domain);
  }

  /**
   * Get domain classification
   */
  public DomainClassification classifyDomain(String email) {
    if (email == null || !email.contains("@")) {
      return DomainClassification.INVALID;
    }

    String domain = email.split("@")[1].toLowerCase();

    if (BLOCKED_DOMAINS.contains(domain)) {
      return DomainClassification.BLOCKED;
    }

    if (SUSPICIOUS_DOMAINS.contains(domain)) {
      return DomainClassification.SUSPICIOUS;
    }

    if (isApprovedCorporateDomain(domain)) {
      return DomainClassification.CORPORATE;
    }

    if (APPROVED_DOMAINS.contains(domain)) {
      return DomainClassification.APPROVED_EXTERNAL;
    }

    return DomainClassification.EXTERNAL;
  }

  /**
   * Domain classification enumeration
   */
  public enum DomainClassification {
    CORPORATE,        // Internal corporate domain
    APPROVED_EXTERNAL, // Approved external domain
    EXTERNAL,         // Unknown external domain
    SUSPICIOUS,       // Flagged as suspicious
    BLOCKED,          // Explicitly blocked
    INVALID           // Invalid email format
  }
}