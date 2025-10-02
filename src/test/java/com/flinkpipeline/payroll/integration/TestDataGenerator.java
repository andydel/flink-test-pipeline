package com.flinkpipeline.payroll.integration;

import com.flinkpipeline.payroll.models.PayrollEmployee;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Test data generator for integration testing.
 * Generates various types of payroll employee records for testing different scenarios.
 */
public class TestDataGenerator {

  private final Random random = new Random();

  private static final String[] FIRST_NAMES = {
      "John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Ashley",
      "William", "Jessica", "James", "Amanda", "Christopher", "Melissa", "Daniel"
  };

  private static final String[] LAST_NAMES = {
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
      "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"
  };

  private static final String[] EMAIL_DOMAINS = {
      "company.com", "corp.company.com", "gmail.com", "outlook.com", "yahoo.com"
  };

  /**
   * Generate valid payroll employee records
   */
  public List<PayrollEmployee> generateValidRecords(int count) {
    List<PayrollEmployee> records = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      PayrollEmployee employee = PayrollEmployee.builder()
          .employeeId(10000 + i)
          .firstName(getRandomFirstName())
          .lastName(getRandomLastName())
          .age(random.nextInt(50) + 18) // Age 18-67
          .ssn(generateValidSSN())
          .hourlyRate(random.nextInt(5000) + 1500) // $15-65/hour in cents
          .gender(random.nextBoolean() ? "M" : "F")
          .email(generateValidEmail())
          .sourceSystem("TEST_SYSTEM")
          .ingestionTimestamp(Instant.now())
          .pipelineVersion("1.0.0-test")
          .build();

      records.add(employee);
    }

    return records;
  }

  /**
   * Generate invalid payroll employee records for error handling tests
   */
  public List<PayrollEmployee> generateInvalidRecords(int count) {
    List<PayrollEmployee> records = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      PayrollEmployee.Builder builder = PayrollEmployee.builder()
          .employeeId(20000 + i);

      // Introduce various validation errors
      switch (i % 5) {
        case 0: // Missing required fields
          builder.firstName("")
                 .lastName(getRandomLastName())
                 .age(25);
          break;
        case 1: // Invalid SSN format
          builder.firstName(getRandomFirstName())
                 .lastName(getRandomLastName())
                 .age(30)
                 .ssn("invalid-ssn-format");
          break;
        case 2: // Invalid age
          builder.firstName(getRandomFirstName())
                 .lastName(getRandomLastName())
                 .age(10); // Too young
          break;
        case 3: // Invalid hourly rate
          builder.firstName(getRandomFirstName())
                 .lastName(getRandomLastName())
                 .age(35)
                 .hourlyRate(100); // Below minimum wage
          break;
        case 4: // Invalid email
          builder.firstName(getRandomFirstName())
                 .lastName(getRandomLastName())
                 .age(40)
                 .email("invalid-email-format");
          break;
      }

      builder.sourceSystem("TEST_SYSTEM")
             .ingestionTimestamp(Instant.now())
             .pipelineVersion("1.0.0-test");

      records.add(builder.build());
    }

    return records;
  }

  /**
   * Generate records with PII data for security testing
   */
  public List<PayrollEmployee> generateRecordsWithPII(int count) {
    List<PayrollEmployee> records = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      PayrollEmployee employee = PayrollEmployee.builder()
          .employeeId(30000 + i)
          .firstName("TestFirstName" + i)
          .lastName("TestLastName" + i)
          .age(random.nextInt(50) + 18)
          .ssn("123-45-67" + String.format("%02d", i % 100)) // Consistent SSN pattern for testing
          .hourlyRate(random.nextInt(5000) + 1500)
          .gender(random.nextBoolean() ? "M" : "F")
          .email("test" + i + "@example.com") // Consistent email pattern for testing
          .sourceSystem("TEST_SYSTEM")
          .ingestionTimestamp(Instant.now())
          .pipelineVersion("1.0.0-test")
          .build();

      records.add(employee);
    }

    return records;
  }

  /**
   * Generate records for performance testing with varied complexity
   */
  public List<PayrollEmployee> generatePerformanceTestRecords(int count) {
    List<PayrollEmployee> records = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      // Create records with varying complexity to test different validation paths
      boolean isComplex = i % 10 == 0; // Every 10th record is more complex

      PayrollEmployee.Builder builder = PayrollEmployee.builder()
          .employeeId(40000 + i)
          .firstName(getRandomFirstName() + (isComplex ? "-ComplexName" : ""))
          .lastName(getRandomLastName() + (isComplex ? "-ComplexLastName" : ""))
          .age(random.nextInt(50) + 18)
          .ssn(generateValidSSN())
          .hourlyRate(random.nextInt(5000) + 1500)
          .gender(random.nextBoolean() ? "M" : "F")
          .email(generateValidEmail())
          .sourceSystem("PERFORMANCE_TEST_SYSTEM")
          .ingestionTimestamp(Instant.now())
          .pipelineVersion("1.0.0-perf");

      records.add(builder.build());
    }

    return records;
  }

  /**
   * Generate records for duplicate detection testing
   */
  public List<PayrollEmployee> generateDuplicateTestRecords(int uniqueCount, int duplicatesPerRecord) {
    List<PayrollEmployee> records = new ArrayList<>();

    // Generate unique records
    List<PayrollEmployee> uniqueRecords = generateValidRecords(uniqueCount);
    records.addAll(uniqueRecords);

    // Generate duplicates
    for (PayrollEmployee original : uniqueRecords) {
      for (int i = 0; i < duplicatesPerRecord; i++) {
        PayrollEmployee duplicate = PayrollEmployee.builder()
            .from(original)
            .employeeId(original.getEmployeeId() + 100000 + i) // Different employee ID
            .ingestionTimestamp(Instant.now().plusSeconds(i * 10)) // Slightly different timestamps
            .build();

        records.add(duplicate);
      }
    }

    return records;
  }

  // Helper methods

  private String getRandomFirstName() {
    return FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
  }

  private String getRandomLastName() {
    return LAST_NAMES[random.nextInt(LAST_NAMES.length)];
  }

  private String generateValidSSN() {
    // Generate valid-format SSN (not real SSN)
    int area = random.nextInt(899) + 100; // 100-999 (avoid 000, 666, 900+)
    int group = random.nextInt(99) + 1;   // 01-99
    int serial = random.nextInt(9999) + 1; // 0001-9999

    return String.format("%03d-%02d-%04d", area, group, serial);
  }

  private String generateValidEmail() {
    String firstName = getRandomFirstName().toLowerCase();
    String lastName = getRandomLastName().toLowerCase();
    String domain = EMAIL_DOMAINS[random.nextInt(EMAIL_DOMAINS.length)];

    return firstName + "." + lastName + "@" + domain;
  }
}