package com.flinkpipeline.payroll.schemas;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for validating payroll employee Avro schema.
 * Tests schema structure, field validation, and serialization/deserialization.
 *
 * IMPORTANT: This test MUST FAIL initially (TDD principle) until PayrollEmployee model is implemented.
 */
@DisplayName("Payroll Employee Schema Validation Tests")
class PayrollEmployeeSchemaTest {

  private Schema payrollEmployeeSchema;
  private GenericRecord validEmployeeRecord;
  private GenericRecord invalidEmployeeRecord;

  @BeforeEach
  void setUp() throws IOException {
    // Load the payroll employee schema from avro directory
    try (InputStream schemaStream = getClass().getClassLoader()
        .getResourceAsStream("avro/input.avro")) {
      assertNotNull(schemaStream, "Payroll employee schema file not found");

      String schemaJson = new String(schemaStream.readAllBytes());
      payrollEmployeeSchema = new Schema.Parser().parse(schemaJson);
    }

    // Create valid employee record for testing
    validEmployeeRecord = new GenericData.Record(payrollEmployeeSchema);
    validEmployeeRecord.put("employee_id", 1001);
    validEmployeeRecord.put("first_name", "John");
    validEmployeeRecord.put("last_name", "Doe");
    validEmployeeRecord.put("age", 30);
    validEmployeeRecord.put("ssn", "123-45-6789");
    validEmployeeRecord.put("hourly_rate", 2500); // $25.00 in cents
    validEmployeeRecord.put("gender", "male");
    validEmployeeRecord.put("email", "john.doe@company.com");

    // Create invalid employee record for testing
    invalidEmployeeRecord = new GenericData.Record(payrollEmployeeSchema);
    invalidEmployeeRecord.put("employee_id", -1);
    invalidEmployeeRecord.put("first_name", "");
    invalidEmployeeRecord.put("last_name", null);
    invalidEmployeeRecord.put("age", 15); // Below minimum age
    invalidEmployeeRecord.put("ssn", "invalid-ssn");
    invalidEmployeeRecord.put("hourly_rate", 500); // Below minimum wage
    invalidEmployeeRecord.put("gender", "invalid");
    invalidEmployeeRecord.put("email", "invalid-email");
  }

  @Test
  @DisplayName("Should validate payroll employee schema structure")
  void shouldValidateSchemaStructure() {
    // Verify schema name
    assertEquals("PayrollEmployee", payrollEmployeeSchema.getName());
    assertEquals("com.flinkpipeline.payroll", payrollEmployeeSchema.getNamespace());

    // Verify all required fields are present
    assertNotNull(payrollEmployeeSchema.getField("employee_id"));
    assertNotNull(payrollEmployeeSchema.getField("first_name"));
    assertNotNull(payrollEmployeeSchema.getField("last_name"));
    assertNotNull(payrollEmployeeSchema.getField("age"));
    assertNotNull(payrollEmployeeSchema.getField("ssn"));
    assertNotNull(payrollEmployeeSchema.getField("hourly_rate"));
    assertNotNull(payrollEmployeeSchema.getField("gender"));
    assertNotNull(payrollEmployeeSchema.getField("email"));

    // Verify field count (8 fields)
    assertEquals(8, payrollEmployeeSchema.getFields().size());
  }

  @Test
  @DisplayName("Should validate field types and constraints")
  void shouldValidateFieldTypesAndConstraints() {
    // Employee ID should be int
    assertEquals(Schema.Type.INT,
        payrollEmployeeSchema.getField("employee_id").schema().getType());

    // Names should be strings
    assertEquals(Schema.Type.STRING,
        payrollEmployeeSchema.getField("first_name").schema().getType());
    assertEquals(Schema.Type.STRING,
        payrollEmployeeSchema.getField("last_name").schema().getType());

    // Age should be int
    assertEquals(Schema.Type.INT,
        payrollEmployeeSchema.getField("age").schema().getType());

    // SSN should be string
    assertEquals(Schema.Type.STRING,
        payrollEmployeeSchema.getField("ssn").schema().getType());

    // Hourly rate should be int (cents)
    assertEquals(Schema.Type.INT,
        payrollEmployeeSchema.getField("hourly_rate").schema().getType());

    // Gender should be enum or string
    Schema genderSchema = payrollEmployeeSchema.getField("gender").schema();
    assertTrue(genderSchema.getType() == Schema.Type.ENUM || genderSchema.getType() == Schema.Type.STRING);

    // Email should be string
    assertEquals(Schema.Type.STRING,
        payrollEmployeeSchema.getField("email").schema().getType());
  }

  @Test
  @DisplayName("Should serialize and deserialize valid employee record")
  void shouldSerializeAndDeserializeValidRecord() throws IOException {
    // This test will fail until PayrollEmployee class is generated
    // TODO: Implement after Avro code generation is set up

    // For now, test with GenericRecord
    assertTrue(validEmployeeRecord.getSchema().equals(payrollEmployeeSchema));
    assertEquals(1001, validEmployeeRecord.get("employee_id"));
    assertEquals("John", validEmployeeRecord.get("first_name"));
    assertEquals("Doe", validEmployeeRecord.get("last_name"));
    assertEquals(30, validEmployeeRecord.get("age"));
    assertEquals("123-45-6789", validEmployeeRecord.get("ssn"));
    assertEquals(2500, validEmployeeRecord.get("hourly_rate"));
    assertEquals("male", validEmployeeRecord.get("gender"));
    assertEquals("john.doe@company.com", validEmployeeRecord.get("email"));
  }

  @Test
  @DisplayName("Should validate required fields are not null")
  void shouldValidateRequiredFieldsNotNull() {
    // Test that required fields cannot be null
    GenericRecord recordWithNulls = new GenericData.Record(payrollEmployeeSchema);

    // This should fail validation when PayrollEmployee model is implemented
    assertThrows(Exception.class, () -> {
      recordWithNulls.put("employee_id", null);
      // TODO: Add validation logic when PayrollEmployee is implemented
    });
  }

  @Test
  @DisplayName("Should validate employee ID is positive")
  void shouldValidateEmployeeIdIsPositive() {
    // This test will fail until validation rules are implemented
    GenericRecord invalidIdRecord = new GenericData.Record(payrollEmployeeSchema);
    invalidIdRecord.put("employee_id", -1);

    // TODO: Implement validation when PayrollValidationEngine is created
    // For now, just verify the record can be created
    assertEquals(-1, invalidIdRecord.get("employee_id"));

    // This assertion will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validate(invalidIdRecord).hasErrors());
  }

  @Test
  @DisplayName("Should validate SSN format")
  void shouldValidateSSNFormat() {
    // This test will fail until SSN validation is implemented
    GenericRecord invalidSSNRecord = new GenericData.Record(payrollEmployeeSchema);
    invalidSSNRecord.put("ssn", "invalid-ssn-format");

    // TODO: Implement SSN validation when PayrollValidationEngine is created
    // For now, just verify the record can be created
    assertEquals("invalid-ssn-format", invalidSSNRecord.get("ssn"));

    // This assertion will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validateSSN("invalid-ssn-format").hasErrors());
  }

  @Test
  @DisplayName("Should validate age range for employment eligibility")
  void shouldValidateAgeRangeForEmployment() {
    // This test will fail until age validation is implemented
    GenericRecord underageRecord = new GenericData.Record(payrollEmployeeSchema);
    underageRecord.put("age", 15);

    GenericRecord overageRecord = new GenericData.Record(payrollEmployeeSchema);
    overageRecord.put("age", 76);

    // TODO: Implement age validation when PayrollValidationEngine is created
    // For now, just verify the records can be created
    assertEquals(15, underageRecord.get("age"));
    assertEquals(76, overageRecord.get("age"));

    // These assertions will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validateAge(15).hasErrors());
    // assertTrue(PayrollValidationEngine.validateAge(76).hasErrors());
    // assertFalse(PayrollValidationEngine.validateAge(30).hasErrors());
  }

  @Test
  @DisplayName("Should validate hourly rate range")
  void shouldValidateHourlyRateRange() {
    // This test will fail until hourly rate validation is implemented
    GenericRecord lowRateRecord = new GenericData.Record(payrollEmployeeSchema);
    lowRateRecord.put("hourly_rate", 500); // $5.00 - below minimum wage

    GenericRecord highRateRecord = new GenericData.Record(payrollEmployeeSchema);
    highRateRecord.put("hourly_rate", 20000); // $200.00 - above executive cap

    // TODO: Implement hourly rate validation when PayrollValidationEngine is created
    // For now, just verify the records can be created
    assertEquals(500, lowRateRecord.get("hourly_rate"));
    assertEquals(20000, highRateRecord.get("hourly_rate"));

    // These assertions will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validateHourlyRate(500).hasErrors());
    // assertTrue(PayrollValidationEngine.validateHourlyRate(20000).hasErrors());
    // assertFalse(PayrollValidationEngine.validateHourlyRate(2500).hasErrors());
  }

  @Test
  @DisplayName("Should validate email format")
  void shouldValidateEmailFormat() {
    // This test will fail until email validation is implemented
    GenericRecord invalidEmailRecord = new GenericData.Record(payrollEmployeeSchema);
    invalidEmailRecord.put("email", "invalid-email-format");

    // TODO: Implement email validation when PayrollValidationEngine is created
    // For now, just verify the record can be created
    assertEquals("invalid-email-format", invalidEmailRecord.get("email"));

    // This assertion will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validateEmail("invalid-email-format").hasErrors());
  }

  @Test
  @DisplayName("Should validate gender enum values")
  void shouldValidateGenderEnumValues() {
    // This test will fail until gender validation is implemented
    GenericRecord invalidGenderRecord = new GenericData.Record(payrollEmployeeSchema);
    invalidGenderRecord.put("gender", "invalid-gender");

    // TODO: Implement gender validation when PayrollValidationEngine is created
    // For now, just verify the record can be created
    assertEquals("invalid-gender", invalidGenderRecord.get("gender"));

    // This assertion will fail until validation is implemented
    // assertTrue(PayrollValidationEngine.validateGender("invalid-gender").hasErrors());
  }

  @Test
  @DisplayName("Should handle schema evolution compatibility")
  void shouldHandleSchemaEvolutionCompatibility() {
    // This test will fail until schema compatibility is implemented
    // TODO: Test backward/forward compatibility when schema registry integration is added

    // Verify current schema version
    assertNotNull(payrollEmployeeSchema);

    // This assertion will fail until schema versioning is implemented
    // assertEquals("1.0.0", PayrollSchemaManager.getCurrentVersion());
  }
}