package com.flinkpipeline.payroll.serialization;

import com.flinkpipeline.payroll.models.PayrollEmployee;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink deserializer for converting Avro-serialized payroll employee records into PayrollEmployee
 * objects. Handles schema evolution, validation, and error recovery for streaming payroll data
 * processing. Integrates with Confluent Schema Registry for schema management and version
 * compatibility.
 */
public class PayrollAvroDeserializer implements DeserializationSchema<PayrollEmployee> {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollAvroDeserializer.class);

  // Schema Registry configuration
  private final String schemaRegistryUrl;
  private final String schemaSubject;
  private final boolean useLatestVersion;

  // Schema management
  private PayrollSchemaManager schemaManager;
  private Schema writerSchema;
  private Schema readerSchema;
  private DatumReader<GenericRecord> datumReader;

  // Deserialization configuration
  private final boolean strictValidation;
  private final boolean enableSchemaEvolution;
  private final boolean logDeserializationErrors;

  // Performance tracking
  private long totalRecordsProcessed = 0;
  private long totalDeserializationErrors = 0;
  private long totalSchemaEvolutionEvents = 0;

  // Constructor
  public PayrollAvroDeserializer(String schemaRegistryUrl, String schemaSubject) {
    this(schemaRegistryUrl, schemaSubject, true, true, true);
  }

  public PayrollAvroDeserializer(
      String schemaRegistryUrl,
      String schemaSubject,
      boolean useLatestVersion,
      boolean strictValidation,
      boolean enableSchemaEvolution) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.schemaSubject = schemaSubject;
    this.useLatestVersion = useLatestVersion;
    this.strictValidation = strictValidation;
    this.enableSchemaEvolution = enableSchemaEvolution;
    this.logDeserializationErrors = true;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    LOG.info(
        "Initializing PayrollAvroDeserializer with schema registry: {}, subject: {}",
        schemaRegistryUrl,
        schemaSubject);

    try {
      // Initialize schema manager
      this.schemaManager = new PayrollSchemaManager(schemaRegistryUrl);

      // Load schemas
      if (useLatestVersion) {
        this.writerSchema = schemaManager.getLatestSchema(schemaSubject);
        LOG.info(
            "Loaded latest schema version {} for subject {}",
            schemaManager.getLatestSchemaVersion(schemaSubject),
            schemaSubject);
      } else {
        this.writerSchema = schemaManager.getSchema(schemaSubject, 1); // Use version 1 as default
        LOG.info("Loaded schema version 1 for subject {}", schemaSubject);
      }

      // Set reader schema (for schema evolution)
      this.readerSchema =
          enableSchemaEvolution ? schemaManager.getPayrollEmployeeReaderSchema() : writerSchema;

      // Initialize datum reader
      this.datumReader = new GenericDatumReader<>(writerSchema, readerSchema);

      LOG.info("PayrollAvroDeserializer initialized successfully");

    } catch (Exception e) {
      LOG.error("Failed to initialize PayrollAvroDeserializer", e);
      throw new RuntimeException("PayrollAvroDeserializer initialization failed", e);
    }
  }

  @Override
  public PayrollEmployee deserialize(byte[] message) throws IOException {
    if (message == null || message.length == 0) {
      if (logDeserializationErrors) {
        LOG.warn("Received null or empty message for deserialization");
      }
      return null;
    }

    totalRecordsProcessed++;

    try {
      // Handle Confluent Schema Registry wire format (magic byte + schema ID + data)
      if (message.length >= 5 && message[0] == 0) {
        return deserializeWithSchemaRegistry(message);
      } else {
        return deserializeWithoutSchemaRegistry(message);
      }

    } catch (Exception e) {
      totalDeserializationErrors++;
      String errorMessage =
          String.format(
              "Failed to deserialize payroll record (total processed: %d, errors: %d): %s",
              totalRecordsProcessed, totalDeserializationErrors, e.getMessage());

      if (strictValidation) {
        LOG.error(errorMessage, e);
        throw new IOException(errorMessage, e);
      } else {
        if (logDeserializationErrors) {
          LOG.warn(errorMessage, e);
        }
        return null; // Return null for invalid records in non-strict mode
      }
    }
  }

  /** Deserialize message with Confluent Schema Registry wire format */
  private PayrollEmployee deserializeWithSchemaRegistry(byte[] message) throws IOException {
    try {
      // Extract schema ID from wire format
      int schemaId = extractSchemaId(message);

      Schema specificWriterSchema;
      try {
        // Get schema for this specific version
        specificWriterSchema = schemaManager.getSchemaById(schemaId);
      } catch (IOException e) {
        LOG.warn(
            "Failed to fetch schema for ID {} from registry. Falling back to latest schema.",
            schemaId,
            e);
        specificWriterSchema = schemaManager.getLatestSchema(schemaSubject);
      }

      // Check if schema evolution is needed
      if (!specificWriterSchema.equals(writerSchema)) {
        totalSchemaEvolutionEvents++;
        LOG.debug("Schema evolution detected: writer schema ID {} differs from expected", schemaId);

        // Create new datum reader for this schema version
        GenericDatumReader<GenericRecord> evolutionReader =
            new GenericDatumReader<>(specificWriterSchema, readerSchema);

        return deserializeWithReader(message, 5, evolutionReader); // Skip 5-byte header
      } else {
        return deserializeWithReader(message, 5, datumReader); // Skip 5-byte header
      }

    } catch (Exception e) {
      throw new IOException("Failed to deserialize with Schema Registry format", e);
    }
  }

  /** Deserialize message without Schema Registry format (plain Avro) */
  private PayrollEmployee deserializeWithoutSchemaRegistry(byte[] message) throws IOException {
    return deserializeWithReader(message, 0, datumReader);
  }

  /** Perform actual deserialization with given reader and offset */
  private PayrollEmployee deserializeWithReader(
      byte[] message, int offset, DatumReader<GenericRecord> reader) throws IOException {
    try {
      // Create binary decoder
      ByteArrayInputStream inputStream =
          new ByteArrayInputStream(message, offset, message.length - offset);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

      // Deserialize to GenericRecord
      GenericRecord record = reader.read(null, decoder);

      // Convert GenericRecord to PayrollEmployee
      return convertToPayrollEmployee(record);

    } catch (Exception e) {
      throw new IOException("Failed to deserialize Avro record", e);
    }
  }

  /** Convert Avro GenericRecord to PayrollEmployee domain object */
  private PayrollEmployee convertToPayrollEmployee(GenericRecord record) {
    try {
      PayrollEmployee.Builder builder = PayrollEmployee.builder();

      // Extract fields with null-safe conversion
      builder
          .employeeId(getIntegerField(record, "employee_id"))
          .firstName(getStringField(record, "first_name"))
          .lastName(getStringField(record, "last_name"))
          .age(getIntegerField(record, "age"))
          .ssn(getStringField(record, "ssn"))
          .hourlyRate(getIntegerField(record, "hourly_rate_cents"))
          .gender(getStringField(record, "gender"))
          .email(getStringField(record, "email"));

      // Add processing metadata
      builder
          .ingestionTimestamp(Instant.now().toEpochMilli())
          .sourceSystem(getStringField(record, "source_system", "KAFKA_AVRO"))
          .pipelineVersion("1.0.0");

      return builder.build();

    } catch (Exception e) {
      throw new RuntimeException("Failed to convert GenericRecord to PayrollEmployee", e);
    }
  }

  /** Extract schema ID from Confluent wire format */
  private int extractSchemaId(byte[] message) {
    if (message.length < 5) {
      throw new IllegalArgumentException("Message too short for Schema Registry format");
    }
    return ((message[1] & 0xFF) << 24)
        | ((message[2] & 0xFF) << 16)
        | ((message[3] & 0xFF) << 8)
        | (message[4] & 0xFF);
  }

  /** Null-safe field extraction helpers */
  private String getStringField(GenericRecord record, String fieldName) {
    Object value = record.get(fieldName);
    return value != null ? value.toString() : null;
  }

  private String getStringField(GenericRecord record, String fieldName, String defaultValue) {
    Object value = record.get(fieldName);
    return value != null ? value.toString() : defaultValue;
  }

  private Integer getIntegerField(GenericRecord record, String fieldName) {
    Object value = record.get(fieldName);
    if (value == null) return null;
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      LOG.warn("Failed to parse integer field {}: {}", fieldName, value);
      return null;
    }
  }

  @Override
  public boolean isEndOfStream(PayrollEmployee nextElement) {
    return false; // Infinite stream
  }

  @Override
  public TypeInformation<PayrollEmployee> getProducedType() {
    return TypeInformation.of(PayrollEmployee.class);
  }

  /** Collect deserialization metrics */
  public DeserializationMetrics getMetrics() {
    return new DeserializationMetrics(
        totalRecordsProcessed,
        totalDeserializationErrors,
        totalSchemaEvolutionEvents,
        calculateErrorRate());
  }

  private double calculateErrorRate() {
    return totalRecordsProcessed > 0
        ? (double) totalDeserializationErrors / totalRecordsProcessed
        : 0.0;
  }

  /** Reset metrics counters */
  public void resetMetrics() {
    totalRecordsProcessed = 0;
    totalDeserializationErrors = 0;
    totalSchemaEvolutionEvents = 0;
  }

  /** Validate record against business rules (optional) */
  private void validateRecord(PayrollEmployee employee) throws IOException {
    if (!strictValidation) return;

    // Basic validation
    if (employee.getEmployeeId() == null || employee.getEmployeeId() <= 0) {
      throw new IOException("Invalid employee ID: " + employee.getEmployeeId());
    }

    if (employee.getFirstName() == null || employee.getFirstName().trim().isEmpty()) {
      throw new IOException("Missing or empty first name");
    }

    if (employee.getLastName() == null || employee.getLastName().trim().isEmpty()) {
      throw new IOException("Missing or empty last name");
    }

    // Additional validations can be added here
  }

  /** Handle schema evolution compatibility issues */
  private void handleSchemaEvolution(
      GenericRecord record, Schema writerSchema, Schema readerSchema) {
    // Log schema differences for monitoring
    if (!writerSchema.equals(readerSchema)) {
      LOG.debug(
          "Schema evolution detected - writer: {}, reader: {}",
          writerSchema.getFullName(),
          readerSchema.getFullName());
    }

    // Handle missing fields with defaults
    for (Schema.Field field : readerSchema.getFields()) {
      if (writerSchema.getField(field.name()) == null && field.hasDefaultValue()) {
        LOG.debug("Using default value for missing field: {}", field.name());
      }
    }
  }

  /** Metrics data class */
  public static class DeserializationMetrics {
    private final long totalRecords;
    private final long totalErrors;
    private final long schemaEvolutions;
    private final double errorRate;

    public DeserializationMetrics(
        long totalRecords, long totalErrors, long schemaEvolutions, double errorRate) {
      this.totalRecords = totalRecords;
      this.totalErrors = totalErrors;
      this.schemaEvolutions = schemaEvolutions;
      this.errorRate = errorRate;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getTotalErrors() {
      return totalErrors;
    }

    public long getSchemaEvolutions() {
      return schemaEvolutions;
    }

    public double getErrorRate() {
      return errorRate;
    }

    @Override
    public String toString() {
      return String.format(
          "DeserializationMetrics{records=%d, errors=%d, schemaEvolutions=%d, errorRate=%.4f}",
          totalRecords, totalErrors, schemaEvolutions, errorRate);
    }
  }

  /** Configuration builder for deserializer */
  public static class Builder {
    private String schemaRegistryUrl;
    private String schemaSubject = "payroll-employee-value";
    private boolean useLatestVersion = true;
    private boolean strictValidation = true;
    private boolean enableSchemaEvolution = true;
    private boolean logDeserializationErrors = true;

    public Builder schemaRegistryUrl(String url) {
      this.schemaRegistryUrl = url;
      return this;
    }

    public Builder schemaSubject(String subject) {
      this.schemaSubject = subject;
      return this;
    }

    public Builder useLatestVersion(boolean useLatest) {
      this.useLatestVersion = useLatest;
      return this;
    }

    public Builder strictValidation(boolean strict) {
      this.strictValidation = strict;
      return this;
    }

    public Builder enableSchemaEvolution(boolean enable) {
      this.enableSchemaEvolution = enable;
      return this;
    }

    public Builder logDeserializationErrors(boolean log) {
      this.logDeserializationErrors = log;
      return this;
    }

    public PayrollAvroDeserializer build() {
      if (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty()) {
        throw new IllegalArgumentException("Schema Registry URL is required");
      }
      return new PayrollAvroDeserializer(
          schemaRegistryUrl,
          schemaSubject,
          useLatestVersion,
          strictValidation,
          enableSchemaEvolution);
    }
  }
}
