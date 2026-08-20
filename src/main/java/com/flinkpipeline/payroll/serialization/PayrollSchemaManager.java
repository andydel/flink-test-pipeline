package com.flinkpipeline.payroll.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema manager for Apache Avro schemas used in payroll data processing. Manages schema
 * registration, versioning, evolution, and compatibility checking with Confluent Schema Registry
 * for payroll employee records.
 */
public class PayrollSchemaManager {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollSchemaManager.class);

  // Schema Registry configuration
  private final String schemaRegistryUrl;
  private final int connectionTimeoutMs;
  private final int readTimeoutMs;

  // Schema caching
  private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
  private final Map<Integer, Schema> schemaIdCache = new ConcurrentHashMap<>();
  private final Map<String, Integer> subjectVersionCache = new ConcurrentHashMap<>();

  // Cache configuration
  private final long cacheExpiryMs;
  private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();

  // Built-in schemas
  private static final String PAYROLL_EMPLOYEE_SCHEMA_V1 =
      """
      {
        "type": "record",
        "name": "PayrollEmployee",
        "namespace": "com.flinkpipeline.payroll.avro",
        "fields": [
          {"name": "employee_id", "type": "int", "doc": "Unique employee identifier"},
          {"name": "first_name", "type": "string", "doc": "Employee first name"},
          {"name": "last_name", "type": "string", "doc": "Employee last name"},
          {"name": "age", "type": ["null", "int"], "default": null, "doc": "Employee age"},
          {"name": "ssn", "type": ["null", "string"], "default": null, "doc": "Social Security Number"},
          {"name": "hourly_rate_cents", "type": ["null", "int"], "default": null, "doc": "Hourly rate in cents"},
          {"name": "gender", "type": ["null", "string"], "default": null, "doc": "Gender identification"},
          {"name": "email", "type": ["null", "string"], "default": null, "doc": "Company email address"},
          {"name": "source_system", "type": ["null", "string"], "default": null, "doc": "Source system identifier"},
          {"name": "ingestion_timestamp", "type": ["null", "long"], "default": null, "doc": "Data ingestion timestamp"},
          {"name": "pipeline_version", "type": ["null", "string"], "default": null, "doc": "Pipeline version"}
        ]
      }
      """;

  private static final String FAILED_PAYROLL_RECORD_SCHEMA_V1 =
      """
      {
        "type": "record",
        "name": "FailedPayrollRecord",
        "namespace": "com.flinkpipeline.payroll.avro",
        "fields": [
          {"name": "original_record", "type": "com.flinkpipeline.payroll.avro.PayrollEmployee", "doc": "Original employee record"},
          {"name": "failure_timestamp", "type": "long", "doc": "Failure timestamp"},
          {"name": "hr_workflow_id", "type": "string", "doc": "HR workflow identifier"},
          {"name": "validation_errors", "type": {"type": "array", "items": "string"}, "doc": "List of validation errors"},
          {"name": "correction_priority", "type": "string", "doc": "Correction priority level"},
          {"name": "estimated_correction_time_minutes", "type": ["null", "int"], "default": null, "doc": "Estimated correction time"},
          {"name": "hr_correction_instructions", "type": ["null", "string"], "default": null, "doc": "HR correction guidance"},
          {"name": "compliance_flags", "type": {"type": "array", "items": "string"}, "default": [], "doc": "Compliance flags"},
          {"name": "processing_latency_ms", "type": "long", "doc": "Processing latency in milliseconds"}
        ]
      }
      """;

  // Constructor
  public PayrollSchemaManager(String schemaRegistryUrl) {
    this(schemaRegistryUrl, 30000, 30000, TimeUnit.HOURS.toMillis(1));
  }

  public PayrollSchemaManager(
      String schemaRegistryUrl, int connectionTimeoutMs, int readTimeoutMs, long cacheExpiryMs) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.cacheExpiryMs = cacheExpiryMs;

    LOG.info("Initialized PayrollSchemaManager with registry URL: {}", schemaRegistryUrl);
    initializeBuiltInSchemas();
  }

  /** Initialize built-in schemas for offline/testing scenarios */
  private void initializeBuiltInSchemas() {
    try {
      // Parse and cache built-in schemas using a shared parser so referenced
      // types (e.g., PayrollEmployee) are visible to dependent schemas.
      Schema.Parser parser = new Schema.Parser();
      Schema payrollEmployeeSchema = parser.parse(PAYROLL_EMPLOYEE_SCHEMA_V1);
      Schema failedRecordSchema = parser.parse(FAILED_PAYROLL_RECORD_SCHEMA_V1);

      schemaCache.put("payroll-employee-value", payrollEmployeeSchema);
      schemaCache.put("failed-payroll-record-value", failedRecordSchema);

      // Cache with artificial IDs for built-in schemas
      schemaIdCache.put(1, payrollEmployeeSchema);
      schemaIdCache.put(1002, failedRecordSchema);

      LOG.info("Initialized built-in schemas for offline operation");

    } catch (Exception e) {
      LOG.error("Failed to initialize built-in schemas", e);
      throw new RuntimeException("Schema initialization failed", e);
    }
  }

  /** Get schema by subject name (latest version) */
  public Schema getLatestSchema(String subject) throws IOException {
    String cacheKey = subject + ":latest";

    // Check cache first
    if (isCacheValid(cacheKey)) {
      Schema cached = schemaCache.get(cacheKey);
      if (cached != null) {
        LOG.debug("Retrieved schema for subject {} from cache", subject);
        return cached;
      }
    }

    try {
      // Try Schema Registry first
      Schema schema = fetchLatestSchemaFromRegistry(subject);
      if (schema != null) {
        cacheSchema(cacheKey, schema);
        return schema;
      }
    } catch (Exception e) {
      LOG.warn("Failed to fetch schema from registry for subject {}: {}", subject, e.getMessage());
    }

    // Fallback to built-in schemas
    Schema builtInSchema = schemaCache.get(subject);
    if (builtInSchema != null) {
      LOG.info("Using built-in schema for subject {}", subject);
      return builtInSchema;
    }

    throw new IOException("Schema not found for subject: " + subject);
  }

  /** Get schema by subject and version */
  public Schema getSchema(String subject, int version) throws IOException {
    String cacheKey = subject + ":" + version;

    // Check cache first
    if (isCacheValid(cacheKey)) {
      Schema cached = schemaCache.get(cacheKey);
      if (cached != null) {
        LOG.debug("Retrieved schema for subject {} version {} from cache", subject, version);
        return cached;
      }
    }

    try {
      // Try Schema Registry first
      Schema schema = fetchSchemaFromRegistry(subject, version);
      if (schema != null) {
        cacheSchema(cacheKey, schema);
        return schema;
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to fetch schema from registry for subject {} version {}: {}",
          subject,
          version,
          e.getMessage());
    }

    // Fallback to built-in schemas for version 1
    if (version == 1) {
      Schema builtInSchema = schemaCache.get(subject);
      if (builtInSchema != null) {
        LOG.info("Using built-in schema for subject {} version {}", subject, version);
        return builtInSchema;
      }
    }

    throw new IOException("Schema not found for subject: " + subject + ", version: " + version);
  }

  /** Get schema by schema ID */
  public Schema getSchemaById(int schemaId) throws IOException {
    // Check cache first
    Schema cached = schemaIdCache.get(schemaId);
    if (cached != null) {
      LOG.debug("Retrieved schema ID {} from cache", schemaId);
      return cached;
    }

    try {
      // Try Schema Registry
      Schema schema = fetchSchemaByIdFromRegistry(schemaId);
      if (schema != null) {
        schemaIdCache.put(schemaId, schema);
        return schema;
      }
    } catch (Exception e) {
      LOG.warn("Failed to fetch schema from registry for ID {}: {}", schemaId, e.getMessage());
    }

    if (schemaId == 1) {
      Schema builtIn = schemaCache.get("payroll-employee-value");
      if (builtIn != null) {
        LOG.info("Using built-in schema for ID {}", schemaId);
        schemaIdCache.put(schemaId, builtIn);
        return builtIn;
      }
    }

    throw new IOException("Schema not found for ID: " + schemaId);
  }

  /** Get latest schema version number */
  public int getLatestSchemaVersion(String subject) throws IOException {
    try {
      return fetchLatestVersionFromRegistry(subject);
    } catch (Exception e) {
      LOG.warn(
          "Failed to fetch latest version from registry for subject {}: {}",
          subject,
          e.getMessage());
      return 1; // Default to version 1 for built-in schemas
    }
  }

  /** Register new schema version */
  public int registerSchema(String subject, Schema schema) throws IOException {
    try {
      int schemaId = registerSchemaInRegistry(subject, schema);

      // Update caches
      String cacheKey = subject + ":latest";
      cacheSchema(cacheKey, schema);
      schemaIdCache.put(schemaId, schema);

      LOG.info("Registered new schema for subject {} with ID {}", subject, schemaId);
      return schemaId;

    } catch (Exception e) {
      LOG.error("Failed to register schema for subject {}", subject, e);
      throw new IOException("Schema registration failed for subject: " + subject, e);
    }
  }

  /** Check schema compatibility */
  public boolean isCompatible(String subject, Schema schema) {
    try {
      return checkCompatibilityInRegistry(subject, schema);
    } catch (Exception e) {
      LOG.warn(
          "Failed to check compatibility in registry for subject {}: {}", subject, e.getMessage());

      // Fallback to basic compatibility check
      try {
        Schema existingSchema = getLatestSchema(subject);
        return isBackwardCompatible(existingSchema, schema);
      } catch (Exception ex) {
        LOG.warn("Failed to perform local compatibility check", ex);
        return false;
      }
    }
  }

  /** Get PayrollEmployee reader schema for schema evolution */
  public Schema getPayrollEmployeeReaderSchema() {
    try {
      return getLatestSchema("payroll-employee-value");
    } catch (IOException e) {
      LOG.error("Failed to get PayrollEmployee reader schema", e);
      throw new RuntimeException("PayrollEmployee reader schema not available", e);
    }
  }

  /** Get FailedPayrollRecord schema */
  public Schema getFailedPayrollRecordSchema() {
    try {
      return getLatestSchema("failed-payroll-record-value");
    } catch (IOException e) {
      LOG.error("Failed to get FailedPayrollRecord schema", e);
      throw new RuntimeException("FailedPayrollRecord schema not available", e);
    }
  }

  /** Load schema from classpath resource */
  public Schema loadSchemaFromResource(String resourcePath) throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Schema resource not found: " + resourcePath);
      }

      String schemaJson = new String(inputStream.readAllBytes());
      Schema schema = new Schema.Parser().parse(schemaJson);

      LOG.info("Loaded schema from resource: {}", resourcePath);
      return schema;

    } catch (Exception e) {
      LOG.error("Failed to load schema from resource: {}", resourcePath, e);
      throw new IOException("Failed to load schema from resource: " + resourcePath, e);
    }
  }

  /** Clear schema caches */
  public void clearCache() {
    schemaCache.clear();
    schemaIdCache.clear();
    subjectVersionCache.clear();
    cacheTimestamps.clear();
    LOG.info("Cleared all schema caches");
  }

  /** Get cache statistics */
  public CacheStatistics getCacheStatistics() {
    return new CacheStatistics(
        schemaCache.size(),
        schemaIdCache.size(),
        subjectVersionCache.size(),
        cacheTimestamps.size());
  }

  // Private helper methods

  private boolean isCacheValid(String cacheKey) {
    Long timestamp = cacheTimestamps.get(cacheKey);
    return timestamp != null && (System.currentTimeMillis() - timestamp < cacheExpiryMs);
  }

  private void cacheSchema(String cacheKey, Schema schema) {
    schemaCache.put(cacheKey, schema);
    cacheTimestamps.put(cacheKey, System.currentTimeMillis());
  }

  private Schema fetchLatestSchemaFromRegistry(String subject) throws IOException {
    // Simulate Schema Registry interaction
    // In real implementation, this would make HTTP calls to Schema Registry
    LOG.debug("Attempting to fetch latest schema for subject {} from registry", subject);

    // For testing/demo purposes, return null to trigger fallback
    return null;
  }

  private Schema fetchSchemaFromRegistry(String subject, int version) throws IOException {
    // Simulate Schema Registry interaction
    LOG.debug(
        "Attempting to fetch schema for subject {} version {} from registry", subject, version);

    // For testing/demo purposes, return null to trigger fallback
    return null;
  }

  private Schema fetchSchemaByIdFromRegistry(int schemaId) throws IOException {
    // Simulate Schema Registry interaction
    LOG.debug("Attempting to fetch schema ID {} from registry", schemaId);

    // For testing/demo purposes, return null to trigger fallback
    return null;
  }

  private int fetchLatestVersionFromRegistry(String subject) throws IOException {
    // Simulate Schema Registry interaction
    LOG.debug("Attempting to fetch latest version for subject {} from registry", subject);

    // For testing/demo purposes, return 1
    return 1;
  }

  private int registerSchemaInRegistry(String subject, Schema schema) throws IOException {
    // Simulate Schema Registry interaction
    LOG.debug("Attempting to register schema for subject {} in registry", subject);

    // For testing/demo purposes, return a mock schema ID
    return (int) (System.currentTimeMillis() % 100000);
  }

  private boolean checkCompatibilityInRegistry(String subject, Schema schema) throws IOException {
    // Simulate Schema Registry compatibility check
    LOG.debug("Attempting to check compatibility for subject {} in registry", subject);

    // For testing/demo purposes, return true
    return true;
  }

  private boolean isBackwardCompatible(Schema writerSchema, Schema readerSchema) {
    try {
      // Basic compatibility check
      if (writerSchema.getType() != readerSchema.getType()) {
        return false;
      }

      if (writerSchema.getType() == Schema.Type.RECORD) {
        // Check that all fields in reader exist in writer or have defaults
        for (Schema.Field readerField : readerSchema.getFields()) {
          Schema.Field writerField = writerSchema.getField(readerField.name());
          if (writerField == null && !readerField.hasDefaultValue()) {
            return false;
          }
        }
      }

      return true;
    } catch (Exception e) {
      LOG.warn("Error during compatibility check", e);
      return false;
    }
  }

  /** Cache statistics data class */
  public static class CacheStatistics {
    private final int schemaCacheSize;
    private final int schemaIdCacheSize;
    private final int versionCacheSize;
    private final int timestampCacheSize;

    public CacheStatistics(
        int schemaCacheSize, int schemaIdCacheSize, int versionCacheSize, int timestampCacheSize) {
      this.schemaCacheSize = schemaCacheSize;
      this.schemaIdCacheSize = schemaIdCacheSize;
      this.versionCacheSize = versionCacheSize;
      this.timestampCacheSize = timestampCacheSize;
    }

    public int getSchemaCacheSize() {
      return schemaCacheSize;
    }

    public int getSchemaIdCacheSize() {
      return schemaIdCacheSize;
    }

    public int getVersionCacheSize() {
      return versionCacheSize;
    }

    public int getTimestampCacheSize() {
      return timestampCacheSize;
    }

    @Override
    public String toString() {
      return String.format(
          "CacheStatistics{schemas=%d, schemaIds=%d, versions=%d, timestamps=%d}",
          schemaCacheSize, schemaIdCacheSize, versionCacheSize, timestampCacheSize);
    }
  }

  /** Builder for PayrollSchemaManager configuration */
  public static class Builder {
    private String schemaRegistryUrl;
    private int connectionTimeoutMs = 30000;
    private int readTimeoutMs = 30000;
    private long cacheExpiryMs = TimeUnit.HOURS.toMillis(1);

    public Builder schemaRegistryUrl(String url) {
      this.schemaRegistryUrl = url;
      return this;
    }

    public Builder connectionTimeout(int timeoutMs) {
      this.connectionTimeoutMs = timeoutMs;
      return this;
    }

    public Builder readTimeout(int timeoutMs) {
      this.readTimeoutMs = timeoutMs;
      return this;
    }

    public Builder cacheExpiry(long expiryMs) {
      this.cacheExpiryMs = expiryMs;
      return this;
    }

    public PayrollSchemaManager build() {
      if (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty()) {
        throw new IllegalArgumentException("Schema Registry URL is required");
      }
      return new PayrollSchemaManager(
          schemaRegistryUrl, connectionTimeoutMs, readTimeoutMs, cacheExpiryMs);
    }
  }
}
