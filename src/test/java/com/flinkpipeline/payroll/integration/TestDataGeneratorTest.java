package com.flinkpipeline.payroll.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flinkpipeline.payroll.models.PayrollEmployee;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDataGeneratorTest {

  // Large enough that, over a uniform 16-75 range, the probability of never
  // sampling either boundary value is negligible (~1e-8), keeping the test
  // both meaningful and non-flaky.
  private static final int SAMPLE_SIZE = 500;

  private final TestDataGenerator generator = new TestDataGenerator();

  @Test
  @DisplayName(
      "generateValidRecords should produce ages spanning the full employment-eligible range (16-75)")
  void shouldGenerateValidRecordsSpanningEmploymentEligibleAgeRange() {
    // Arrange & Act
    List<PayrollEmployee> records = generator.generateValidRecords(SAMPLE_SIZE);

    // Assert
    assertAgesWithinAndSpanningRange(records);
  }

  @Test
  @DisplayName(
      "generateRecordsWithPII should produce ages spanning the full employment-eligible range (16-75)")
  void shouldGeneratePiiRecordsSpanningEmploymentEligibleAgeRange() {
    // Arrange & Act
    List<PayrollEmployee> records = generator.generateRecordsWithPII(SAMPLE_SIZE);

    // Assert
    assertAgesWithinAndSpanningRange(records);
  }

  @Test
  @DisplayName(
      "generatePerformanceTestRecords should produce ages spanning the full employment-eligible range (16-75)")
  void shouldGeneratePerformanceRecordsSpanningEmploymentEligibleAgeRange() {
    // Arrange & Act
    List<PayrollEmployee> records = generator.generatePerformanceTestRecords(SAMPLE_SIZE);

    // Assert
    assertAgesWithinAndSpanningRange(records);
  }

  private void assertAgesWithinAndSpanningRange(List<PayrollEmployee> records) {
    int minAge = records.stream().mapToInt(PayrollEmployee::getAge).min().orElseThrow();
    int maxAge = records.stream().mapToInt(PayrollEmployee::getAge).max().orElseThrow();

    for (PayrollEmployee employee : records) {
      int age = employee.getAge();
      assertTrue(
          age >= 16 && age <= 75, "Expected generated age to be within 16-75 but was " + age);
    }

    assertTrue(
        minAge <= 17,
        "Expected sample to include ages down to the 16-17 floor, lowest seen was " + minAge);
    assertTrue(
        maxAge >= 74,
        "Expected sample to include ages up to the 74-75 ceiling, highest seen was " + maxAge);
  }
}
