package com.flinkpipeline.payroll;

import com.flinkpipeline.payroll.models.FieldValidationResult;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import com.flinkpipeline.payroll.models.PayrollValidationResult;
import com.flinkpipeline.payroll.validation.PayrollRuleEngine;

/**
 * Quick test to verify core implementations are working
 */
public class QuickTest {

  public static void main(String[] args) {
    System.out.println("Testing Payroll Data Quality Pipeline Core Implementation...");

    // Create test employee
    PayrollEmployee employee = PayrollEmployee.builder()
        .employeeId(1001)
        .firstName("John")
        .lastName("Doe")
        .age(30)
        .ssn("123-45-6789")
        .hourlyRateFromDollars(25.00)
        .gender("male")
        .email("john.doe@company.com")
        .build();

    System.out.println("Created test employee: " + employee);

    // Test validation engine
    PayrollRuleEngine engine = new PayrollRuleEngine(false); // Sequential for testing
    PayrollValidationResult result = engine.validate(employee);

    System.out.println("\nValidation Result:");
    System.out.println("Overall Status: " + result.getOverallStatus());
    System.out.println("Processing Latency: " + result.getProcessingLatencyMs() + "ms");
    System.out.println("Field Results Count: " + result.getFieldResults().size());

    for (FieldValidationResult fieldResult : result.getFieldResults()) {
      System.out.println("  " + fieldResult.getFieldName() + ": " + fieldResult.getStatus() +
          (fieldResult.isFailed() ? " - " + fieldResult.getErrorMessage() : ""));
    }

    // Test invalid employee
    System.out.println("\n--- Testing Invalid Employee ---");
    PayrollEmployee invalidEmployee = PayrollEmployee.builder()
        .employeeId(-1)
        .firstName("")
        .lastName(null)
        .age(15)
        .ssn("invalid-ssn")
        .hourlyRateFromDollars(5.00)
        .gender("invalid")
        .email("invalid-email")
        .build();

    PayrollValidationResult invalidResult = engine.validate(invalidEmployee);
    System.out.println("Invalid Employee Status: " + invalidResult.getOverallStatus());
    System.out.println("Error Count: " + invalidResult.getFailureCount());
    System.out.println("Errors:");
    for (String error : invalidResult.getErrorMessages()) {
      System.out.println("  - " + error);
    }

    engine.shutdown();
    System.out.println("\nCore implementation test completed successfully!");
  }
}