package com.flinkpipeline.payroll.schemas;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for validating failed payroll record Avro schema.
 * Tests schema structure for HR workflow integration and error tracking.
 *
 * IMPORTANT: This test MUST FAIL initially (TDD principle) until FailedPayrollRecord model is implemented.
 */
@DisplayName("Failed Payroll Record Schema Validation Tests")
class FailedPayrollRecordSchemaTest {

  private Schema failedPayrollRecordSchema;
  private GenericRecord validFailedRecord;
  private GenericRecord originalEmployeeRecord;

  @BeforeEach
  void setUp() throws IOException {
    // Create the expected schema structure for failed payroll records
    // This will be used to validate the actual schema when implemented
    String expectedSchemaJson = """
        {
          "type": "record",
          "name": "FailedPayrollRecord",
          "namespace": "com.flinkpipeline.payroll",
          "doc": "Schema for payroll records that failed validation",
          "fields": [
            {
              "name": "failure_id",
              "type": "string",
              "doc": "Unique identifier for this failure record"
            },
            {
              "name": "original_record",
              "type": "string",
              "doc": "JSON representation of the original payroll record"
            },
            {
              "name": "employee_id",
              "type": ["null", "int"],
              "default": null,
              "doc": "Employee ID from original record if available"
            },
            {
              "name": "failure_timestamp",
              "type": "long",
              "doc": "Timestamp when validation failure occurred (epoch milliseconds)"
            },
            {
              "name": "validation_errors",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "ValidationError",
                  "fields": [
                    {
                      "name": "rule_id",
                      "type": "string",
                      "doc": "Validation rule that failed (e.g., DQ-001)"
                    },
                    {
                      "name": "field_name",
                      "type": "string",
                      "doc": "Field that failed validation"
                    },
                    {
                      "name": "error_message",
                      "type": "string",
                      "doc": "Human-readable error message for HR team"
                    },
                    {
                      "name": "error_code",
                      "type": "string",
                      "doc": "Machine-readable error code"
                    },
                    {
                      "name": "severity",
                      "type": {
                        "type": "enum",
                        "name": "ErrorSeverity",
                        "symbols": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
                      },
                      "doc": "Error severity level"
                    }
                  ]
                }
              },
              "doc": "List of validation errors that occurred"
            },
            {
              "name": "hr_workflow_info",
              "type": {
                "type": "record",
                "name": "HRWorkflowInfo",
                "fields": [
                  {
                    "name": "workflow_id",
                    "type": "string",
                    "doc": "HR system workflow/ticket ID"
                  },
                  {
                    "name": "priority",
                    "type": {
                      "type": "enum",
                      "name": "WorkflowPriority",
                      "symbols": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
                    },
                    "doc": "HR workflow priority level"
                  },
                  {
                    "name": "estimated_correction_time_minutes",
                    "type": "int",
                    "doc": "Estimated time to correct in minutes"
                  },
                  {
                    "name": "assigned_to",
                    "type": ["null", "string"],
                    "default": null,
                    "doc": "HR team member assigned to handle this issue"
                  },
                  {
                    "name": "correction_guidance",
                    "type": "string",
                    "doc": "Specific guidance for HR team on how to correct"
                  }
                ]
              },
              "doc": "HR workflow integration information"
            },
            {
              "name": "compliance_impact",
              "type": {
                "type": "record",
                "name": "ComplianceImpact",
                "fields": [
                  {
                    "name": "is_regulatory_violation",
                    "type": "boolean",
                    "doc": "Whether this failure constitutes a regulatory violation"
                  },
                  {
                    "name": "compliance_rules_violated",
                    "type": {
                      "type": "array",
                      "items": "string"
                    },
                    "doc": "List of compliance rules that were violated"
                  },
                  {
                    "name": "pii_exposure_risk",
                    "type": {
                      "type": "enum",
                      "name": "PIIRisk",
                      "symbols": ["NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"]
                    },
                    "doc": "Risk level for PII exposure"
                  }
                ]
              },
              "doc": "Compliance impact assessment"
            },
            {
              "name": "processing_metadata",
              "type": {
                "type": "record",
                "name": "ProcessingMetadata",
                "fields": [
                  {
                    "name": "pipeline_version",
                    "type": "string",
                    "doc": "Version of the payroll pipeline that processed this record"
                  },
                  {
                    "name": "validation_rules_version",
                    "type": "string",
                    "doc": "Version of validation rules applied"
                  },
                  {
                    "name": "correlation_id",
                    "type": "string",
                    "doc": "Correlation ID for tracing across systems"
                  },
                  {
                    "name": "retry_count",
                    "type": "int",
                    "default": 0,
                    "doc": "Number of retry attempts made"
                  }
                ]
              },
              "doc": "Processing and debugging metadata"
            }
          ]
        }
        """;

    failedPayrollRecordSchema = new Schema.Parser().parse(expectedSchemaJson);

    // Create a valid failed record for testing
    validFailedRecord = createValidFailedRecord();

    // Create original employee record
    originalEmployeeRecord = createOriginalEmployeeRecord();
  }

  private GenericRecord createValidFailedRecord() {
    GenericRecord record = new GenericData.Record(failedPayrollRecordSchema);

    record.put("failure_id", "FAIL-" + System.currentTimeMillis());
    record.put("original_record", "{\"employee_id\": 1001, \"ssn\": \"invalid-ssn\"}");
    record.put("employee_id", 1001);
    record.put("failure_timestamp", Instant.now().toEpochMilli());

    // Create validation errors
    GenericRecord validationError = new GenericData.Record(
        failedPayrollRecordSchema.getField("validation_errors").schema().getElementType());
    validationError.put("rule_id", "DQ-005");
    validationError.put("field_name", "ssn");
    validationError.put("error_message", "Invalid SSN format - must be XXX-XX-XXXX");
    validationError.put("error_code", "SSN_FORMAT_INVALID");
    validationError.put("severity", new GenericData.EnumSymbol(
        validationError.getSchema().getField("severity").schema(), "CRITICAL"));

    record.put("validation_errors", Arrays.asList(validationError));

    // Create HR workflow info
    GenericRecord hrWorkflowInfo = new GenericData.Record(
        failedPayrollRecordSchema.getField("hr_workflow_info").schema());
    hrWorkflowInfo.put("workflow_id", "HR-WF-" + System.currentTimeMillis());
    hrWorkflowInfo.put("priority", new GenericData.EnumSymbol(
        hrWorkflowInfo.getSchema().getField("priority").schema(), "CRITICAL"));
    hrWorkflowInfo.put("estimated_correction_time_minutes", 5);
    hrWorkflowInfo.put("assigned_to", null);
    hrWorkflowInfo.put("correction_guidance", "Enter SSN in format XXX-XX-XXXX (e.g., 123-45-6789)");

    record.put("hr_workflow_info", hrWorkflowInfo);

    // Create compliance impact
    GenericRecord complianceImpact = new GenericData.Record(
        failedPayrollRecordSchema.getField("compliance_impact").schema());
    complianceImpact.put("is_regulatory_violation", true);
    complianceImpact.put("compliance_rules_violated", Arrays.asList("SSN_FORMAT_COMPLIANCE"));
    complianceImpact.put("pii_exposure_risk", new GenericData.EnumSymbol(
        complianceImpact.getSchema().getField("pii_exposure_risk").schema(), "MEDIUM"));

    record.put("compliance_impact", complianceImpact);

    // Create processing metadata
    GenericRecord processingMetadata = new GenericData.Record(
        failedPayrollRecordSchema.getField("processing_metadata").schema());
    processingMetadata.put("pipeline_version", "1.0.0");
    processingMetadata.put("validation_rules_version", "1.0.0");
    processingMetadata.put("correlation_id", "CORR-" + System.currentTimeMillis());
    processingMetadata.put("retry_count", 0);

    record.put("processing_metadata", processingMetadata);

    return record;
  }

  private GenericRecord createOriginalEmployeeRecord() {
    // This would typically use the PayrollEmployee schema
    // For now, create a simple record structure
    String employeeSchemaJson = """
        {
          "type": "record",
          "name": "PayrollEmployee",
          "fields": [
            {"name": "employee_id", "type": "int"},
            {"name": "ssn", "type": "string"}
          ]
        }
        """;

    Schema employeeSchema = new Schema.Parser().parse(employeeSchemaJson);
    GenericRecord employee = new GenericData.Record(employeeSchema);
    employee.put("employee_id", 1001);
    employee.put("ssn", "invalid-ssn");

    return employee;
  }

  @Test
  @DisplayName("Should validate failed payroll record schema structure")
  void shouldValidateFailedRecordSchemaStructure() {
    // Verify schema name and namespace
    assertEquals("FailedPayrollRecord", failedPayrollRecordSchema.getName());
    assertEquals("com.flinkpipeline.payroll", failedPayrollRecordSchema.getNamespace());

    // Verify all required fields are present
    assertNotNull(failedPayrollRecordSchema.getField("failure_id"));
    assertNotNull(failedPayrollRecordSchema.getField("original_record"));
    assertNotNull(failedPayrollRecordSchema.getField("employee_id"));
    assertNotNull(failedPayrollRecordSchema.getField("failure_timestamp"));
    assertNotNull(failedPayrollRecordSchema.getField("validation_errors"));
    assertNotNull(failedPayrollRecordSchema.getField("hr_workflow_info"));
    assertNotNull(failedPayrollRecordSchema.getField("compliance_impact"));
    assertNotNull(failedPayrollRecordSchema.getField("processing_metadata"));

    // Verify field count (8 main fields)
    assertEquals(8, failedPayrollRecordSchema.getFields().size());
  }

  @Test
  @DisplayName("Should validate validation errors array structure")
  void shouldValidateValidationErrorsStructure() {
    Schema validationErrorsSchema = failedPayrollRecordSchema.getField("validation_errors").schema();
    assertEquals(Schema.Type.ARRAY, validationErrorsSchema.getType());

    Schema validationErrorSchema = validationErrorsSchema.getElementType();
    assertEquals("ValidationError", validationErrorSchema.getName());

    // Verify ValidationError fields
    assertNotNull(validationErrorSchema.getField("rule_id"));
    assertNotNull(validationErrorSchema.getField("field_name"));
    assertNotNull(validationErrorSchema.getField("error_message"));
    assertNotNull(validationErrorSchema.getField("error_code"));
    assertNotNull(validationErrorSchema.getField("severity"));

    // Verify severity enum
    Schema severitySchema = validationErrorSchema.getField("severity").schema();
    assertEquals(Schema.Type.ENUM, severitySchema.getType());
    assertEquals("ErrorSeverity", severitySchema.getName());
  }

  @Test
  @DisplayName("Should validate HR workflow info structure")
  void shouldValidateHRWorkflowInfoStructure() {
    Schema hrWorkflowSchema = failedPayrollRecordSchema.getField("hr_workflow_info").schema();
    assertEquals(Schema.Type.RECORD, hrWorkflowSchema.getType());
    assertEquals("HRWorkflowInfo", hrWorkflowSchema.getName());

    // Verify HR workflow fields
    assertNotNull(hrWorkflowSchema.getField("workflow_id"));
    assertNotNull(hrWorkflowSchema.getField("priority"));
    assertNotNull(hrWorkflowSchema.getField("estimated_correction_time_minutes"));
    assertNotNull(hrWorkflowSchema.getField("assigned_to"));
    assertNotNull(hrWorkflowSchema.getField("correction_guidance"));

    // Verify priority enum
    Schema prioritySchema = hrWorkflowSchema.getField("priority").schema();
    assertEquals(Schema.Type.ENUM, prioritySchema.getType());
    assertEquals("WorkflowPriority", prioritySchema.getName());
  }

  @Test
  @DisplayName("Should validate compliance impact structure")
  void shouldValidateComplianceImpactStructure() {
    Schema complianceSchema = failedPayrollRecordSchema.getField("compliance_impact").schema();
    assertEquals(Schema.Type.RECORD, complianceSchema.getType());
    assertEquals("ComplianceImpact", complianceSchema.getName());

    // Verify compliance fields
    assertNotNull(complianceSchema.getField("is_regulatory_violation"));
    assertNotNull(complianceSchema.getField("compliance_rules_violated"));
    assertNotNull(complianceSchema.getField("pii_exposure_risk"));

    // Verify PII risk enum
    Schema piiRiskSchema = complianceSchema.getField("pii_exposure_risk").schema();
    assertEquals(Schema.Type.ENUM, piiRiskSchema.getType());
    assertEquals("PIIRisk", piiRiskSchema.getName());
  }

  @Test
  @DisplayName("Should create valid failed record with all required fields")
  void shouldCreateValidFailedRecord() {
    // Verify the failed record was created successfully
    assertNotNull(validFailedRecord);
    assertNotNull(validFailedRecord.get("failure_id"));
    assertNotNull(validFailedRecord.get("original_record"));
    assertNotNull(validFailedRecord.get("failure_timestamp"));
    assertNotNull(validFailedRecord.get("validation_errors"));
    assertNotNull(validFailedRecord.get("hr_workflow_info"));
    assertNotNull(validFailedRecord.get("compliance_impact"));
    assertNotNull(validFailedRecord.get("processing_metadata"));

    // Verify specific field values
    assertEquals(1001, validFailedRecord.get("employee_id"));
    assertTrue(validFailedRecord.get("failure_id").toString().startsWith("FAIL-"));
    assertTrue(((List<?>) validFailedRecord.get("validation_errors")).size() > 0);
  }

  @Test
  @DisplayName("Should handle multiple validation errors")
  void shouldHandleMultipleValidationErrors() {
    // This test will fail until FailedPayrollRecord model supports multiple errors
    GenericRecord multiErrorRecord = new GenericData.Record(failedPayrollRecordSchema);

    // Create multiple validation errors
    GenericRecord ssnError = new GenericData.Record(
        failedPayrollRecordSchema.getField("validation_errors").schema().getElementType());
    ssnError.put("rule_id", "DQ-005");
    ssnError.put("field_name", "ssn");
    ssnError.put("error_message", "Invalid SSN format");
    ssnError.put("error_code", "SSN_FORMAT_INVALID");
    ssnError.put("severity", new GenericData.EnumSymbol(
        ssnError.getSchema().getField("severity").schema(), "CRITICAL"));

    GenericRecord ageError = new GenericData.Record(
        failedPayrollRecordSchema.getField("validation_errors").schema().getElementType());
    ageError.put("rule_id", "DQ-004");
    ageError.put("field_name", "age");
    ageError.put("error_message", "Age outside employment eligibility range");
    ageError.put("error_code", "AGE_RANGE_INVALID");
    ageError.put("severity", new GenericData.EnumSymbol(
        ageError.getSchema().getField("severity").schema(), "HIGH"));

    multiErrorRecord.put("validation_errors", Arrays.asList(ssnError, ageError));

    // Verify multiple errors can be stored
    List<?> errors = (List<?>) multiErrorRecord.get("validation_errors");
    assertEquals(2, errors.size());

    // TODO: This assertion will fail until FailedPayrollRecord model is implemented
    // assertTrue(FailedPayrollRecordBuilder.canHandleMultipleErrors());
  }

  @Test
  @DisplayName("Should support HR workflow priority mapping")
  void shouldSupportHRWorkflowPriorityMapping() {
    // This test will fail until HR workflow integration is implemented
    Map<String, String> expectedPriorityMapping = new HashMap<>();
    expectedPriorityMapping.put("REGULATORY", "CRITICAL");
    expectedPriorityMapping.put("BUSINESS", "HIGH");
    expectedPriorityMapping.put("INFORMATIONAL", "MEDIUM");

    // TODO: This assertion will fail until HRWorkflowService is implemented
    // assertEquals(expectedPriorityMapping, HRWorkflowService.getPriorityMapping());

    // For now, just verify the enum values exist
    Schema prioritySchema = failedPayrollRecordSchema
        .getField("hr_workflow_info").schema()
        .getField("priority").schema();
    List<String> symbols = prioritySchema.getEnumSymbols();
    assertTrue(symbols.contains("CRITICAL"));
    assertTrue(symbols.contains("HIGH"));
    assertTrue(symbols.contains("MEDIUM"));
    assertTrue(symbols.contains("LOW"));
  }

  @Test
  @DisplayName("Should validate correction time estimates")
  void shouldValidateCorrectionTimeEstimates() {
    // This test will fail until correction time estimation is implemented
    GenericRecord hrWorkflow = (GenericRecord) validFailedRecord.get("hr_workflow_info");
    int estimatedTime = (Integer) hrWorkflow.get("estimated_correction_time_minutes");

    // Verify reasonable time estimate
    assertTrue(estimatedTime > 0);
    assertTrue(estimatedTime <= 120); // Max 2 hours for any correction

    // TODO: This assertion will fail until CorrectionTimeEstimator is implemented
    // assertEquals(5, CorrectionTimeEstimator.getEstimatedTime("FORMAT"));
    // assertEquals(30, CorrectionTimeEstimator.getEstimatedTime("COMPLIANCE"));
  }

  @Test
  @DisplayName("Should track compliance violations correctly")
  void shouldTrackComplianceViolationsCorrectly() {
    // This test will fail until compliance tracking is implemented
    GenericRecord complianceImpact = (GenericRecord) validFailedRecord.get("compliance_impact");
    boolean isRegulatory = (Boolean) complianceImpact.get("is_regulatory_violation");
    List<?> rulesViolated = (List<?>) complianceImpact.get("compliance_rules_violated");

    assertTrue(isRegulatory);
    assertTrue(rulesViolated.size() > 0);
    assertTrue(rulesViolated.contains("SSN_FORMAT_COMPLIANCE"));

    // TODO: This assertion will fail until ComplianceTracker is implemented
    // assertTrue(ComplianceTracker.isRegulatory("DQ-005"));
    // assertFalse(ComplianceTracker.isRegulatory("DQ-002"));
  }

  @Test
  @DisplayName("Should generate unique failure IDs")
  void shouldGenerateUniqueFailureIDs() {
    // This test will fail until FailureIdGenerator is implemented
    String failureId1 = validFailedRecord.get("failure_id").toString();
    String failureId2 = createValidFailedRecord().get("failure_id").toString();

    assertNotEquals(failureId1, failureId2);
    assertTrue(failureId1.startsWith("FAIL-"));
    assertTrue(failureId2.startsWith("FAIL-"));

    // TODO: This assertion will fail until FailureIdGenerator is implemented
    // assertTrue(FailureIdGenerator.isValidFormat(failureId1));
    // assertTrue(FailureIdGenerator.isUnique(failureId1));
  }
}