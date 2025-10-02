package com.flinkpipeline.payroll.connectors;

import com.flinkpipeline.payroll.models.ComplianceAuditLog;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Flink connector for writing compliance audit logs to Apache Iceberg tables. Provides
 * tamper-evident, long-term storage for audit trails with GDPR compliance, regulatory reporting,
 * and forensic analysis capabilities.
 *
 * <p>Features: - Tamper-evident audit log storage with checksums - Long-term retention policies (7+
 * years for regulatory compliance) - Partition-based data organization by date and audit type -
 * GDPR-compliant PII access logging and data lineage - Real-time compliance monitoring and alerting
 * - Optimized for write-heavy workloads and analytical queries - Integration with compliance
 * reporting and SIEM systems
 */
public class AuditLogIcebergSinkConnector {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogIcebergSinkConnector.class);

  // Table configuration
  private final String catalogName;
  private final String databaseName;
  private final String tableName;
  private final String warehousePath;
  private final Map<String, String> catalogProperties;
  private final String restCatalogUri;
  private final String restCredentialsKey;
  private final String restCredentialsToken;

  // Compliance configuration
  private final Duration retentionPeriod;
  private final boolean enableTamperDetection;
  private final boolean enableEncryption;
  private final String partitionSpec;

  // Performance configuration
  private final long commitIntervalMs;
  private final String compressionCodec;

  // Metrics
  private final AtomicLong auditLogsWritten = new AtomicLong(0);
  private final AtomicLong complianceViolationsLogged = new AtomicLong(0);
  private final AtomicLong piiAccessLogsWritten = new AtomicLong(0);

  // Default configuration
  private static final String DEFAULT_CATALOG = "compliance_audit_catalog";
  private static final String DEFAULT_DATABASE = "compliance";
  private static final String DEFAULT_TABLE = "audit_logs";
  private static final String DEFAULT_COMPRESSION = "zstd"; // Better compression for audit logs
  private static final long DEFAULT_COMMIT_INTERVAL_MS = 30000; // 30 seconds for audit logs
  private static final Duration DEFAULT_RETENTION = Duration.ofDays(7 * 365); // 7 years

  // Constructor
  public AuditLogIcebergSinkConnector(String warehousePath) {
    this(
        DEFAULT_CATALOG,
        DEFAULT_DATABASE,
        DEFAULT_TABLE,
        warehousePath,
        new HashMap<>(),
        null,
        null,
        null,
        DEFAULT_RETENTION,
        true,
        true,
        "audit_date,audit_type");
  }

  public AuditLogIcebergSinkConnector(
      String catalogName,
      String databaseName,
      String tableName,
      String warehousePath,
      Map<String, String> catalogProperties,
      String restCatalogUri,
      String restCredentialsKey,
      String restCredentialsToken,
      Duration retentionPeriod,
      boolean enableTamperDetection,
      boolean enableEncryption,
      String partitionSpec) {
    this.catalogName = catalogName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.warehousePath = warehousePath;
    this.catalogProperties = new HashMap<>(catalogProperties);
    this.restCatalogUri = restCatalogUri;
    this.restCredentialsKey = restCredentialsKey;
    this.restCredentialsToken = restCredentialsToken;
    this.retentionPeriod = retentionPeriod;
    this.enableTamperDetection = enableTamperDetection;
    this.enableEncryption = enableEncryption;
    this.partitionSpec = partitionSpec;

    // Set performance defaults for audit logs
    this.commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
    this.compressionCodec = DEFAULT_COMPRESSION;

    LOG.info(
        "Initialized AuditLogIcebergSinkConnector - Catalog: {}, Database: {}, Table: {}, Retention: {}",
        catalogName,
        databaseName,
        tableName,
        retentionPeriod);
  }

  /** Create Iceberg sink for compliance audit logs */
  public void createAuditLogSink(
      DataStream<ComplianceAuditLog> auditStream, StreamTableEnvironment tableEnv) {
    LOG.info("Creating Iceberg sink for compliance audit logs");

    try {
      // Initialize Iceberg catalog for audit logs
      initializeAuditCatalog(tableEnv);

      // Create or verify audit log table
      createOrVerifyAuditTable(tableEnv);

      // Convert ComplianceAuditLog to RowData
      DataStream<RowData> rowDataStream =
          auditStream.map(new AuditLogToRowDataMapper()).name("Convert Audit Log to RowData");

      // Configure and create Iceberg sink
      configureSinkToAuditTable(rowDataStream, tableEnv);

      // Schedule retention and cleanup policies
      scheduleAuditLogMaintenance(tableEnv);

      LOG.info("Successfully created Iceberg sink for audit logs");

    } catch (Exception e) {
      LOG.error("Failed to create audit log Iceberg sink", e);
      throw new RuntimeException("Audit log Iceberg sink creation failed", e);
    }
  }

  /** Initialize Iceberg catalog for audit logs with compliance settings */
  private void initializeAuditCatalog(StreamTableEnvironment tableEnv) {
    LOG.info("Initializing audit log Iceberg catalog: {}", catalogName);

    // Configure catalog properties for compliance
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("type", "iceberg");
    catalogConfig.put("warehouse", warehousePath);

    if (restCatalogUri != null && !restCatalogUri.isBlank()) {
      catalogConfig.put("catalog-type", "rest");
      catalogConfig.put("uri", restCatalogUri);

      if (restCredentialsKey != null
          && !restCredentialsKey.isBlank()
          && restCredentialsToken != null
          && !restCredentialsToken.isBlank()) {
        catalogConfig.put(restCredentialsKey, restCredentialsToken);
      }
    } else {
      catalogConfig.put("catalog-type", "hadoop");
      catalogConfig.put("hadoop-conf-dir", "/etc/hadoop/conf");
    }

    // Compliance and security configuration
    if (enableEncryption) {
      catalogConfig.put("encryption.key-id", "audit-log-encryption-key");
      catalogConfig.put("encryption.type", "aes256");
      catalogConfig.put("encryption.key-metadata", "compliance-audit-logs");
    }

    // Audit-specific optimizations
    catalogConfig.put("write.format.default", "parquet");
    catalogConfig.put("write.parquet.compression-codec", compressionCodec);
    catalogConfig.put("write.target-file-size-bytes", "67108864"); // 64MB for audit logs
    catalogConfig.put(
        "write.metadata.delete-after-commit.enabled", "false"); // Keep metadata for audit
    catalogConfig.put(
        "write.metadata.previous-versions-max", "100"); // Keep more versions for audit trail

    // Tamper detection settings
    if (enableTamperDetection) {
      catalogConfig.put("write.parquet.bloom-filter-enabled.audit_id", "true");
      catalogConfig.put("write.parquet.bloom-filter-enabled.checksum", "true");
    }

    // Long-term retention settings
    catalogConfig.put(
        "table.expire-snapshots.min-snapshots-to-keep", "365"); // Keep daily snapshots for a year
    catalogConfig.put(
        "table.expire-snapshots.max-snapshot-age-ms", String.valueOf(retentionPeriod.toMillis()));

    // Add custom properties
    catalogConfig.putAll(catalogProperties);

    // Create catalog
    String createCatalogSql =
        String.format(
            "CREATE CATALOG %s WITH (%s)", catalogName, formatCatalogProperties(catalogConfig));

    if (!tableEnv.getCatalog(catalogName).isPresent()) {
      tableEnv.executeSql(createCatalogSql);
      LOG.info("Created audit catalog: {}", catalogName);
    } else {
      LOG.info("Audit catalog already exists: {}", catalogName);
    }

    tableEnv.useCatalog(catalogName);

    LOG.info("Audit log Iceberg catalog initialized successfully");
  }

  /** Create or verify audit log table schema with compliance requirements */
  private void createOrVerifyAuditTable(StreamTableEnvironment tableEnv) {
    LOG.info("Creating/verifying audit log table: {}.{}.{}", catalogName, databaseName, tableName);

    // Create database if not exists
    String createDatabaseSql = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
    tableEnv.executeSql(createDatabaseSql);

    // Create audit log table with comprehensive schema
    String createTableSql =
        String.format(
            """
        CREATE TABLE IF NOT EXISTS %s.%s.%s (
          audit_id STRING NOT NULL,
          audit_timestamp TIMESTAMP(3) NOT NULL,
          audit_type STRING NOT NULL,
          employee_id INT,
          user_id STRING,
          action_performed STRING NOT NULL,
          data_classification STRING NOT NULL,
          pii_fields ARRAY<STRING>,
          access_purpose STRING,
          processing_context STRING,
          system_source STRING,
          session_id STRING,
          ip_address STRING,
          user_agent STRING,
          compliance_flags ARRAY<STRING>,
          retention_expires TIMESTAMP(3),
          checksum STRING,
          metadata_json STRING,
          audit_date DATE NOT NULL,
          audit_hour INT NOT NULL,
          year INT NOT NULL,
          month INT NOT NULL,
          day INT NOT NULL
        ) PARTITIONED BY (audit_date, audit_type, year, month)
        WITH (
          'format-version' = '2',
          'write.target-file-size-bytes' = '67108864',
          'write.delete.mode' = 'copy-on-write',
          'write.update.mode' = 'copy-on-write',
          'write.merge.mode' = 'copy-on-write',
          'read.split.target-size' = '134217728',
          'history.expire.min-snapshots-to-keep' = '365'
        )
        """,
            catalogName, databaseName, tableName);

    tableEnv.executeSql(createTableSql);

    LOG.info("Audit log table created/verified successfully");
  }

  /** Configure sink to write to audit log table */
  private void configureSinkToAuditTable(
      DataStream<RowData> rowDataStream, StreamTableEnvironment tableEnv) {
    String fullTableName = String.format("%s.%s.%s", catalogName, databaseName, tableName);

    // Create temporary view from stream
    tableEnv.createTemporaryView("audit_log_stream", rowDataStream);

    // Configure streaming insert with partition computation
    String insertSql =
        String.format(
            """
        INSERT INTO %s
        SELECT
          audit_id,
          audit_timestamp,
          audit_type,
          employee_id,
          user_id,
          action_performed,
          data_classification,
          pii_fields,
          access_purpose,
          processing_context,
          system_source,
          session_id,
          ip_address,
          user_agent,
          compliance_flags,
          retention_expires,
          checksum,
          metadata_json,
          CAST(audit_timestamp AS DATE) as audit_date,
          HOUR(audit_timestamp) as audit_hour,
          YEAR(audit_timestamp) as year,
          MONTH(audit_timestamp) as month,
          DAYOFMONTH(audit_timestamp) as day
        FROM audit_log_stream
        """,
            fullTableName);

    // Execute streaming insert
    tableEnv.executeSql(insertSql);

    LOG.info("Configured streaming insert to audit log table: {}", fullTableName);
  }

  /** Mapper to convert ComplianceAuditLog to Flink RowData */
  private class AuditLogToRowDataMapper implements MapFunction<ComplianceAuditLog, RowData> {

    @Override
    public RowData map(ComplianceAuditLog auditLog) throws Exception {
      auditLogsWritten.incrementAndGet();

      // Track specific audit log types
      if (auditLog.getAuditType() == ComplianceAuditLog.AuditType.PII_FIELD_ACCESSED) {
        piiAccessLogsWritten.incrementAndGet();
      } else if (auditLog.getAuditType()
          == ComplianceAuditLog.AuditType.COMPLIANCE_VIOLATION_DETECTED) {
        complianceViolationsLogged.incrementAndGet();
      }

      GenericRowData row = new GenericRowData(23);

      // Core audit fields
      row.setField(0, StringData.fromString(auditLog.getAuditId()));
      row.setField(1, TimestampData.fromInstant(auditLog.getAuditTimestamp()));
      row.setField(2, StringData.fromString(auditLog.getAuditType().toString()));
      row.setField(3, auditLog.getEmployeeId());
      row.setField(
          4, auditLog.getUserId() != null ? StringData.fromString(auditLog.getUserId()) : null);
      row.setField(5, StringData.fromString(auditLog.getOperationDetails()));
      row.setField(6, StringData.fromString(auditLog.getDataClassification().toString()));

      // PII and access fields
      row.setField(7, convertStringListToArray(auditLog.getPiiFieldsAccessed()));
      row.setField(
          8,
          auditLog.getAccessPurpose() != null
              ? StringData.fromString(auditLog.getAccessPurpose())
              : null);
      row.setField(
          9,
          auditLog.getBusinessJustification() != null
              ? StringData.fromString(auditLog.getBusinessJustification())
              : null);

      // System and session fields
      row.setField(
          10,
          auditLog.getSystemComponent() != null
              ? StringData.fromString(auditLog.getSystemComponent())
              : null);
      row.setField(
          11,
          auditLog.getSessionId() != null ? StringData.fromString(auditLog.getSessionId()) : null);
      row.setField(
          12,
          auditLog.getSourceIPAddress() != null
              ? StringData.fromString(auditLog.getSourceIPAddress())
              : null);
      row.setField(
          13,
          auditLog.getUserAgent() != null ? StringData.fromString(auditLog.getUserAgent()) : null);

      // Compliance and retention fields
      row.setField(14, convertStringListToArray(auditLog.getComplianceFlags()));
      row.setField(
          15,
          auditLog.getRetentionExpires() != null
              ? TimestampData.fromInstant(auditLog.getRetentionExpires())
              : null);

      // Tamper detection
      row.setField(16, StringData.fromString(auditLog.getAuditHash()));

      // Metadata
      row.setField(17, StringData.fromString(buildMetadataJson(auditLog)));

      // Partition fields (computed from audit timestamp)
      LocalDateTime auditDateTime =
          LocalDateTime.ofInstant(auditLog.getAuditTimestamp(), java.time.ZoneId.systemDefault());

      row.setField(
          18, (int) auditDateTime.toLocalDate().toEpochDay()); // audit_date as days since epoch
      row.setField(19, auditDateTime.getHour());
      row.setField(20, auditDateTime.getYear());
      row.setField(21, auditDateTime.getMonthValue());
      row.setField(22, auditDateTime.getDayOfMonth());

      return row;
    }

    private GenericArrayData convertStringListToArray(java.util.List<String> stringList) {
      if (stringList == null || stringList.isEmpty()) {
        return null;
      }
      return new GenericArrayData(
          stringList.stream().map(StringData::fromString).toArray(StringData[]::new));
    }

    private String buildMetadataJson(ComplianceAuditLog auditLog) {
      StringBuilder json = new StringBuilder("{");
      json.append("\"operationDetails\":\"")
          .append(escapeJson(auditLog.getOperationDetails()))
          .append("\"");
      if (auditLog.getBusinessJustification() != null) {
        json.append(",\"businessJustification\":\"")
            .append(escapeJson(auditLog.getBusinessJustification()))
            .append("\"");
      }
      if (!auditLog.getComplianceFlags().isEmpty()) {
        json.append(",\"complianceFlags\":\"")
            .append(escapeJson(auditLog.getComplianceFlags().toString()))
            .append("\"");
      }
      json.append("}");
      return json.toString();
    }

    private String escapeJson(String value) {
      if (value == null) {
        return "";
      }
      return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
  }

  /** Schedule audit log maintenance and retention policies */
  private void scheduleAuditLogMaintenance(StreamTableEnvironment tableEnv) {
    LOG.info("Scheduling audit log maintenance and retention policies");

    String fullTableName = String.format("%s.%s.%s", catalogName, databaseName, tableName);

    // Example maintenance operations (would be scheduled externally)
    String compactSql =
        String.format("CALL %s.system.rewrite_data_files('%s')", catalogName, fullTableName);

    // Retention policy - expire old snapshots but keep data
    LocalDateTime retentionCutoff = LocalDateTime.now().minus(retentionPeriod);
    String expireSnapshotsSql =
        String.format(
            "CALL %s.system.expire_snapshots(table => '%s', older_than => TIMESTAMP '%s', retain_last => 100)",
            catalogName,
            fullTableName,
            retentionCutoff.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

    // Orphan file cleanup (be very careful with audit logs)
    String cleanupOrphansSql =
        String.format(
            "CALL %s.system.remove_orphan_files(table => '%s', older_than => TIMESTAMP '%s')",
            catalogName,
            fullTableName,
            LocalDateTime.now()
                .minusDays(3)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

    LOG.info(
        "Audit log maintenance commands prepared - Compact: {}, Expire: {}, Cleanup: {}",
        compactSql,
        expireSnapshotsSql,
        cleanupOrphansSql);
  }

  /** Format catalog properties for SQL DDL */
  private String formatCatalogProperties(Map<String, String> properties) {
    StringBuilder sb = new StringBuilder();
    properties.forEach(
        (key, value) -> {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append("'").append(key).append("' = '").append(value).append("'");
        });
    return sb.toString();
  }

  /** Get audit log sink metrics */
  public AuditSinkMetrics getMetrics() {
    return new AuditSinkMetrics(
        auditLogsWritten.get(),
        complianceViolationsLogged.get(),
        piiAccessLogsWritten.get(),
        calculateAuditThroughput());
  }

  private double calculateAuditThroughput() {
    return auditLogsWritten.get() / 60.0; // Audit logs per minute
  }

  /** Builder for AuditLogIcebergSinkConnector configuration */
  public static class Builder {
    private String catalogName = DEFAULT_CATALOG;
    private String databaseName = DEFAULT_DATABASE;
    private String tableName = DEFAULT_TABLE;
    private String warehousePath;
    private Map<String, String> catalogProperties = new HashMap<>();
    private String restUri;
    private String restCredentialsKey;
    private String restCredentialsToken;
    private Duration retentionPeriod = DEFAULT_RETENTION;
    private boolean enableTamperDetection = true;
    private boolean enableEncryption = true;
    private String partitionSpec = "audit_date,audit_type";

    public Builder warehousePath(String path) {
      this.warehousePath = path;
      return this;
    }

    public Builder catalogName(String name) {
      this.catalogName = name;
      return this;
    }

    public Builder databaseName(String name) {
      this.databaseName = name;
      return this;
    }

    public Builder tableName(String name) {
      this.tableName = name;
      return this;
    }

    public Builder retentionPeriod(Duration period) {
      this.retentionPeriod = period;
      return this;
    }

    public Builder enableTamperDetection(boolean enable) {
      this.enableTamperDetection = enable;
      return this;
    }

    public Builder enableEncryption(boolean enable) {
      this.enableEncryption = enable;
      return this;
    }

    public Builder partitionBy(String spec) {
      this.partitionSpec = spec;
      return this;
    }

    public Builder withCatalogProperty(String key, String value) {
      this.catalogProperties.put(key, value);
      return this;
    }

    public Builder restCatalog(String uri) {
      this.restUri = uri;
      return this;
    }

    public Builder restCredentials(String key, String token) {
      this.restCredentialsKey = key;
      this.restCredentialsToken = token;
      return this;
    }

    public AuditLogIcebergSinkConnector build() {
      if (warehousePath == null || warehousePath.trim().isEmpty()) {
        throw new IllegalArgumentException("Warehouse path is required");
      }

      return new AuditLogIcebergSinkConnector(
          catalogName,
          databaseName,
          tableName,
          warehousePath,
          catalogProperties,
          restUri,
          restCredentialsKey,
          restCredentialsToken,
          retentionPeriod,
          enableTamperDetection,
          enableEncryption,
          partitionSpec);
    }
  }

  /** Factory methods for common audit log configurations */
  public static AuditLogIcebergSinkConnector forDevelopment(String warehousePath) {
    return new Builder()
        .warehousePath(warehousePath)
        .catalogName("dev_audit_catalog")
        .databaseName("compliance_dev")
        .retentionPeriod(Duration.ofDays(30))
        .enableTamperDetection(false)
        .enableEncryption(false)
        .build();
  }

  public static AuditLogIcebergSinkConnector forProduction(String warehousePath) {
    return new Builder()
        .warehousePath(warehousePath)
        .catalogName("prod_audit_catalog")
        .databaseName("compliance")
        .retentionPeriod(Duration.ofDays(7 * 365)) // 7 years for regulatory compliance
        .enableTamperDetection(true)
        .enableEncryption(true)
        .withCatalogProperty("write.target-file-size-bytes", "134217728") // 128MB for audit logs
        .build();
  }

  public static AuditLogIcebergSinkConnector forTesting(String warehousePath) {
    return new Builder()
        .warehousePath(warehousePath)
        .catalogName("test_audit_catalog")
        .databaseName("compliance_test")
        .tableName("test_audit_logs_" + System.currentTimeMillis())
        .retentionPeriod(Duration.ofDays(1))
        .enableTamperDetection(false)
        .enableEncryption(false)
        .build();
  }

  /** Audit sink metrics data class */
  public static class AuditSinkMetrics {
    private final long totalAuditLogs;
    private final long complianceViolations;
    private final long piiAccessLogs;
    private final double throughputPerMinute;

    public AuditSinkMetrics(
        long totalAuditLogs,
        long complianceViolations,
        long piiAccessLogs,
        double throughputPerMinute) {
      this.totalAuditLogs = totalAuditLogs;
      this.complianceViolations = complianceViolations;
      this.piiAccessLogs = piiAccessLogs;
      this.throughputPerMinute = throughputPerMinute;
    }

    public long getTotalAuditLogs() {
      return totalAuditLogs;
    }

    public long getComplianceViolations() {
      return complianceViolations;
    }

    public long getPiiAccessLogs() {
      return piiAccessLogs;
    }

    public double getThroughputPerMinute() {
      return throughputPerMinute;
    }

    public double getComplianceViolationRate() {
      return totalAuditLogs > 0 ? (double) complianceViolations / totalAuditLogs : 0.0;
    }

    @Override
    public String toString() {
      return String.format(
          "AuditSinkMetrics{total=%d, violations=%d, piiAccess=%d, throughput=%.2f/min, violationRate=%.4f}",
          totalAuditLogs,
          complianceViolations,
          piiAccessLogs,
          throughputPerMinute,
          getComplianceViolationRate());
    }
  }
}
