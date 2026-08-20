package com.flinkpipeline.payroll.validation.rules;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.FieldValidationResult.ComplianceLevel;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Validation rule for detecting duplicate employees in payroll processing. Implements windowed
 * duplicate detection using SSN, email, and name similarity algorithms with configurable time
 * windows and similarity thresholds.
 */
public class DuplicateDetectionRule {

  // Detection configuration
  private static final Duration DEFAULT_DETECTION_WINDOW = Duration.ofHours(1);
  private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(15);
  private static final double NAME_SIMILARITY_THRESHOLD = 0.8;
  private static final boolean ENABLE_NAME_SIMILARITY = true;
  private static final boolean ENABLE_FUZZY_MATCHING = true;

  // In-memory storage for duplicate detection (in production, use distributed cache)
  private final ConcurrentMap<String, EmployeeRecord> ssnIndex = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, EmployeeRecord> emailIndex = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<EmployeeRecord>> nameIndex = new ConcurrentHashMap<>();

  // Configuration
  private final Duration detectionWindow;
  private final boolean enableNameSimilarity;
  private final double nameSimilarityThreshold;

  // Metrics
  private long totalProcessed = 0;
  private long duplicatesDetected = 0;
  private long ssnDuplicates = 0;
  private long emailDuplicates = 0;
  private long nameSimilarityMatches = 0;

  // Last cleanup timestamp
  private volatile Instant lastCleanup = Instant.now();

  // Constructor
  public DuplicateDetectionRule() {
    this(DEFAULT_DETECTION_WINDOW, ENABLE_NAME_SIMILARITY, NAME_SIMILARITY_THRESHOLD);
  }

  public DuplicateDetectionRule(Duration detectionWindow) {
    this(detectionWindow, ENABLE_NAME_SIMILARITY, NAME_SIMILARITY_THRESHOLD);
  }

  public DuplicateDetectionRule(
      Duration detectionWindow, boolean enableNameSimilarity, double nameSimilarityThreshold) {
    this.detectionWindow = detectionWindow;
    this.enableNameSimilarity = enableNameSimilarity;
    this.nameSimilarityThreshold = nameSimilarityThreshold;
  }

  /** Check for duplicate employee based on multiple criteria */
  public FieldValidationResult checkDuplicate(PayrollEmployee employee) {
    if (employee == null) {
      return FieldValidationResult.failure(
          "employee_id",
          "Duplicate Detection",
          "Employee record is null",
          "Provide valid employee record for duplicate checking",
          ComplianceLevel.BUSINESS);
    }

    totalProcessed++;

    // Periodic cleanup of expired records
    if (shouldPerformCleanup()) {
      performCleanup();
    }

    // Create employee record for tracking
    EmployeeRecord record = new EmployeeRecord(employee);

    // Check for exact SSN match
    DuplicateMatch ssnMatch = checkSSNDuplicate(record);
    if (ssnMatch != null) {
      ssnDuplicates++;
      duplicatesDetected++;
      return createDuplicateFailureResult(employee, ssnMatch, "SSN_DUPLICATE");
    }

    // Check for exact email match
    DuplicateMatch emailMatch = checkEmailDuplicate(record);
    if (emailMatch != null) {
      emailDuplicates++;
      duplicatesDetected++;
      return createDuplicateFailureResult(employee, emailMatch, "EMAIL_DUPLICATE");
    }

    // Check for name similarity (if enabled)
    if (enableNameSimilarity) {
      DuplicateMatch nameMatch = checkNameSimilarity(record);
      if (nameMatch != null) {
        nameSimilarityMatches++;
        // Name similarity is a warning, not a failure
        return createSimilarityWarningResult(employee, nameMatch);
      }
    }

    // No duplicates found - add to indices for future checks
    addToIndices(record);

    return FieldValidationResult.success("employee_id", "Duplicate Detection");
  }

  /** Check for SSN-based duplicates */
  private DuplicateMatch checkSSNDuplicate(EmployeeRecord newRecord) {
    if (newRecord.ssn == null || newRecord.ssn.trim().isEmpty()) {
      return null; // Cannot check duplicates without SSN
    }

    String normalizedSSN = normalizeSSN(newRecord.ssn);
    EmployeeRecord existingRecord = ssnIndex.get(normalizedSSN);

    if (existingRecord != null && !isExpired(existingRecord)) {
      return new DuplicateMatch(
          existingRecord,
          DuplicateType.SSN_DUPLICATE,
          1.0, // Exact match
          "Exact SSN match found",
          generateResolutionGuidance("SSN", existingRecord, newRecord));
    }

    return null;
  }

  /** Check for email-based duplicates */
  private DuplicateMatch checkEmailDuplicate(EmployeeRecord newRecord) {
    if (newRecord.email == null || newRecord.email.trim().isEmpty()) {
      return null; // Cannot check duplicates without email
    }

    String normalizedEmail = normalizeEmail(newRecord.email);
    EmployeeRecord existingRecord = emailIndex.get(normalizedEmail);

    if (existingRecord != null && !isExpired(existingRecord)) {
      return new DuplicateMatch(
          existingRecord,
          DuplicateType.EMAIL_DUPLICATE,
          1.0, // Exact match
          "Exact email match found",
          generateResolutionGuidance("EMAIL", existingRecord, newRecord));
    }

    return null;
  }

  /** Check for name similarity duplicates */
  private DuplicateMatch checkNameSimilarity(EmployeeRecord newRecord) {
    if (newRecord.firstName == null || newRecord.lastName == null) {
      return null; // Cannot check similarity without names
    }

    String nameKey = generateNameKey(newRecord.firstName, newRecord.lastName);
    List<EmployeeRecord> existingRecords = nameIndex.get(nameKey);

    if (existingRecords != null) {
      for (EmployeeRecord existingRecord : existingRecords) {
        if (isExpired(existingRecord)) continue;

        double similarity = calculateNameSimilarity(newRecord, existingRecord);
        if (similarity >= nameSimilarityThreshold) {
          return new DuplicateMatch(
              existingRecord,
              DuplicateType.NAME_SIMILARITY,
              similarity,
              String.format("Name similarity %.2f%% detected", similarity * 100),
              generateResolutionGuidance("NAME_SIMILARITY", existingRecord, newRecord));
        }
      }
    }

    return null;
  }

  /** Add record to all relevant indices */
  private void addToIndices(EmployeeRecord record) {
    // Add to SSN index
    if (record.ssn != null && !record.ssn.trim().isEmpty()) {
      String normalizedSSN = normalizeSSN(record.ssn);
      ssnIndex.put(normalizedSSN, record);
    }

    // Add to email index
    if (record.email != null && !record.email.trim().isEmpty()) {
      String normalizedEmail = normalizeEmail(record.email);
      emailIndex.put(normalizedEmail, record);
    }

    // Add to name index
    if (record.firstName != null && record.lastName != null) {
      String nameKey = generateNameKey(record.firstName, record.lastName);
      nameIndex.computeIfAbsent(nameKey, k -> new ArrayList<>()).add(record);
    }
  }

  /** Create failure result for duplicate detection */
  private FieldValidationResult createDuplicateFailureResult(
      PayrollEmployee employee, DuplicateMatch match, String duplicateType) {
    String errorMessage =
        String.format(
            "Duplicate employee detected: %s (Confidence: %.1f%%, Original Employee ID: %d)",
            match.reason, match.confidence * 100, match.originalRecord.employeeId);

    String correctionGuidance =
        String.format(
            "DUPLICATE DETECTED - %s\n\n%s\n\nRecommended Action: %s",
            duplicateType, match.resolutionGuidance, getRecommendedAction(match.type));

    return FieldValidationResult.failure(
        "employee_id",
        "Duplicate Detection",
        errorMessage,
        correctionGuidance,
        ComplianceLevel.BUSINESS);
  }

  /** Create warning result for name similarity */
  private FieldValidationResult createSimilarityWarningResult(
      PayrollEmployee employee, DuplicateMatch match) {
    String warningMessage =
        String.format(
            "Potential duplicate detected: %s (Similarity: %.1f%%, Employee ID: %d)",
            match.reason, match.confidence * 100, match.originalRecord.employeeId);

    String correctionGuidance =
        String.format(
            "POTENTIAL DUPLICATE - NAME SIMILARITY\n\n%s\n\nRecommended Action: Manual review recommended",
            match.resolutionGuidance);

    return FieldValidationResult.warning(
        "employee_id",
        "Duplicate Detection",
        warningMessage,
        correctionGuidance,
        ComplianceLevel.BUSINESS);
  }

  /** Generate resolution guidance for duplicate handling */
  private String generateResolutionGuidance(
      String duplicateType, EmployeeRecord existing, EmployeeRecord newRecord) {
    StringBuilder guidance = new StringBuilder();

    guidance.append("DUPLICATE EMPLOYEE DETECTED\n\n");
    guidance.append("Existing Employee:\n");
    guidance.append("- Employee ID: ").append(existing.employeeId).append("\n");
    guidance
        .append("- Name: ")
        .append(existing.firstName)
        .append(" ")
        .append(existing.lastName)
        .append("\n");
    guidance.append("- Email: ").append(existing.email).append("\n");
    guidance.append("- SSN: ").append(maskSSN(existing.ssn)).append("\n");
    guidance.append("- Processed: ").append(existing.timestamp).append("\n\n");

    guidance.append("New Employee:\n");
    guidance.append("- Employee ID: ").append(newRecord.employeeId).append("\n");
    guidance
        .append("- Name: ")
        .append(newRecord.firstName)
        .append(" ")
        .append(newRecord.lastName)
        .append("\n");
    guidance.append("- Email: ").append(newRecord.email).append("\n");
    guidance.append("- SSN: ").append(maskSSN(newRecord.ssn)).append("\n\n");

    switch (duplicateType) {
      case "SSN":
        guidance.append("CRITICAL: SSN duplication may indicate:\n");
        guidance.append("- Data entry error\n");
        guidance.append("- Identity theft or fraud\n");
        guidance.append("- System error or duplicate submission\n");
        break;
      case "EMAIL":
        guidance.append("EMAIL duplication may indicate:\n");
        guidance.append("- Shared email account (not recommended)\n");
        guidance.append("- Data entry error\n");
        guidance.append("- Employee record update needed\n");
        break;
      case "NAME_SIMILARITY":
        guidance.append("NAME similarity may indicate:\n");
        guidance.append("- Related employees (family members)\n");
        guidance.append("- Similar names (coincidence)\n");
        guidance.append("- Data entry variations\n");
        break;
    }

    return guidance.toString();
  }

  /** Get recommended action based on duplicate type */
  private String getRecommendedAction(DuplicateType type) {
    switch (type) {
      case SSN_DUPLICATE:
        return "IMMEDIATE REVIEW REQUIRED - Verify employee identity and investigate potential fraud";
      case EMAIL_DUPLICATE:
        return "UPDATE EMAIL - Assign unique email address to employee";
      case NAME_SIMILARITY:
        return "MANUAL REVIEW - Verify if employees are distinct individuals";
      default:
        return "REVIEW AND RESOLVE - Investigate duplicate and take appropriate action";
    }
  }

  /** Calculate name similarity using Levenshtein distance and fuzzy matching */
  private double calculateNameSimilarity(EmployeeRecord record1, EmployeeRecord record2) {
    // Calculate similarity for first names
    double firstNameSimilarity =
        calculateStringSimilarity(
            normalizeNameForComparison(record1.firstName),
            normalizeNameForComparison(record2.firstName));

    // Calculate similarity for last names
    double lastNameSimilarity =
        calculateStringSimilarity(
            normalizeNameForComparison(record1.lastName),
            normalizeNameForComparison(record2.lastName));

    // Combined similarity (weighted average)
    return (firstNameSimilarity * 0.4) + (lastNameSimilarity * 0.6);
  }

  /** Calculate string similarity using normalized edit distance */
  private double calculateStringSimilarity(String s1, String s2) {
    if (s1 == null || s2 == null) return 0.0;
    if (s1.equals(s2)) return 1.0;

    int editDistance = levenshteinDistance(s1, s2);
    int maxLength = Math.max(s1.length(), s2.length());

    return maxLength == 0 ? 1.0 : 1.0 - ((double) editDistance / maxLength);
  }

  /** Calculate Levenshtein distance between two strings */
  private int levenshteinDistance(String s1, String s2) {
    int[][] dp = new int[s1.length() + 1][s2.length() + 1];

    for (int i = 0; i <= s1.length(); i++) {
      dp[i][0] = i;
    }
    for (int j = 0; j <= s2.length(); j++) {
      dp[0][j] = j;
    }

    for (int i = 1; i <= s1.length(); i++) {
      for (int j = 1; j <= s2.length(); j++) {
        int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
        dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
      }
    }

    return dp[s1.length()][s2.length()];
  }

  /** Normalize string for name comparison */
  private String normalizeNameForComparison(String name) {
    if (name == null) return "";
    return name.toLowerCase().trim().replaceAll("[\\s\\-']+", "");
  }

  /** Normalize SSN for comparison */
  private String normalizeSSN(String ssn) {
    if (ssn == null) return "";
    return ssn.replaceAll("[^0-9]", "");
  }

  /** Normalize email for comparison */
  private String normalizeEmail(String email) {
    if (email == null) return "";
    return email.toLowerCase().trim();
  }

  /** Generate name key for indexing */
  private String generateNameKey(String firstName, String lastName) {
    if (firstName == null || lastName == null) return "";
    String first = firstName.toLowerCase().trim();
    String last = lastName.toLowerCase().trim();
    return first.charAt(0) + ":" + last.charAt(0); // First letter of each name
  }

  /** Mask SSN for logging/display purposes */
  private String maskSSN(String ssn) {
    if (ssn == null || ssn.length() < 4) return "***-**-****";
    return "***-**-" + ssn.substring(ssn.length() - 4);
  }

  /** Check if record is expired based on detection window */
  private boolean isExpired(EmployeeRecord record) {
    return Instant.now().isAfter(record.timestamp.plus(detectionWindow));
  }

  /** Check if cleanup should be performed */
  private boolean shouldPerformCleanup() {
    return Instant.now().isAfter(lastCleanup.plus(CLEANUP_INTERVAL));
  }

  /** Perform cleanup of expired records */
  private void performCleanup() {
    Instant now = Instant.now();
    Instant cutoff = now.minus(detectionWindow);

    // Clean SSN index
    ssnIndex.entrySet().removeIf(entry -> entry.getValue().timestamp.isBefore(cutoff));

    // Clean email index
    emailIndex.entrySet().removeIf(entry -> entry.getValue().timestamp.isBefore(cutoff));

    // Clean name index
    nameIndex
        .entrySet()
        .removeIf(
            entry -> {
              entry.getValue().removeIf(record -> record.timestamp.isBefore(cutoff));
              return entry.getValue().isEmpty();
            });

    lastCleanup = now;
  }

  /** Get detection metrics */
  public DuplicateDetectionMetrics getMetrics() {
    return new DuplicateDetectionMetrics(
        totalProcessed,
        duplicatesDetected,
        ssnDuplicates,
        emailDuplicates,
        nameSimilarityMatches,
        calculateDuplicateRate(),
        ssnIndex.size(),
        emailIndex.size(),
        nameIndex.size());
  }

  private double calculateDuplicateRate() {
    return totalProcessed > 0 ? (double) duplicatesDetected / totalProcessed : 0.0;
  }

  /** Reset metrics */
  public void resetMetrics() {
    totalProcessed = 0;
    duplicatesDetected = 0;
    ssnDuplicates = 0;
    emailDuplicates = 0;
    nameSimilarityMatches = 0;
  }

  /** Clear all detection indices */
  public void clearIndices() {
    ssnIndex.clear();
    emailIndex.clear();
    nameIndex.clear();
    lastCleanup = Instant.now();
  }

  // Data classes

  /** Employee record for duplicate detection */
  private static class EmployeeRecord {
    final Integer employeeId;
    final String firstName;
    final String lastName;
    final String ssn;
    final String email;
    final Instant timestamp;

    EmployeeRecord(PayrollEmployee employee) {
      this.employeeId = employee.getEmployeeId();
      this.firstName = employee.getFirstName();
      this.lastName = employee.getLastName();
      this.ssn = employee.getSsn();
      this.email = employee.getEmail();
      this.timestamp = Instant.now();
    }
  }

  /** Duplicate match result */
  private static class DuplicateMatch {
    final EmployeeRecord originalRecord;
    final DuplicateType type;
    final double confidence;
    final String reason;
    final String resolutionGuidance;

    DuplicateMatch(
        EmployeeRecord originalRecord,
        DuplicateType type,
        double confidence,
        String reason,
        String resolutionGuidance) {
      this.originalRecord = originalRecord;
      this.type = type;
      this.confidence = confidence;
      this.reason = reason;
      this.resolutionGuidance = resolutionGuidance;
    }
  }

  /** Duplicate type enumeration */
  public enum DuplicateType {
    SSN_DUPLICATE,
    EMAIL_DUPLICATE,
    NAME_SIMILARITY,
    EXACT_MATCH
  }

  /** Metrics data class */
  public static class DuplicateDetectionMetrics {
    private final long totalProcessed;
    private final long duplicatesDetected;
    private final long ssnDuplicates;
    private final long emailDuplicates;
    private final long nameSimilarityMatches;
    private final double duplicateRate;
    private final int ssnIndexSize;
    private final int emailIndexSize;
    private final int nameIndexSize;

    public DuplicateDetectionMetrics(
        long totalProcessed,
        long duplicatesDetected,
        long ssnDuplicates,
        long emailDuplicates,
        long nameSimilarityMatches,
        double duplicateRate,
        int ssnIndexSize,
        int emailIndexSize,
        int nameIndexSize) {
      this.totalProcessed = totalProcessed;
      this.duplicatesDetected = duplicatesDetected;
      this.ssnDuplicates = ssnDuplicates;
      this.emailDuplicates = emailDuplicates;
      this.nameSimilarityMatches = nameSimilarityMatches;
      this.duplicateRate = duplicateRate;
      this.ssnIndexSize = ssnIndexSize;
      this.emailIndexSize = emailIndexSize;
      this.nameIndexSize = nameIndexSize;
    }

    // Getters
    public long getTotalProcessed() {
      return totalProcessed;
    }

    public long getDuplicatesDetected() {
      return duplicatesDetected;
    }

    public long getSsnDuplicates() {
      return ssnDuplicates;
    }

    public long getEmailDuplicates() {
      return emailDuplicates;
    }

    public long getNameSimilarityMatches() {
      return nameSimilarityMatches;
    }

    public double getDuplicateRate() {
      return duplicateRate;
    }

    public int getSsnIndexSize() {
      return ssnIndexSize;
    }

    public int getEmailIndexSize() {
      return emailIndexSize;
    }

    public int getNameIndexSize() {
      return nameIndexSize;
    }

    @Override
    public String toString() {
      return String.format(
          "DuplicateDetectionMetrics{processed=%d, duplicates=%d, rate=%.4f, indices=[ssn=%d, email=%d, name=%d]}",
          totalProcessed,
          duplicatesDetected,
          duplicateRate,
          ssnIndexSize,
          emailIndexSize,
          nameIndexSize);
    }
  }
}
