-- Iceberg table schema for validated payroll employee records
-- Partitioned by ingestion time (year/month/day/hour) for optimal query performance

CREATE TABLE payroll_data_lake.validated_employees (
    -- Employee identification
    employee_id INT NOT NULL COMMENT 'Unique employee identifier',

    -- Personal information
    first_name STRING NOT NULL COMMENT 'Employee first name (validated format)',
    last_name STRING NOT NULL COMMENT 'Employee last name (validated format)',
    age INT NOT NULL COMMENT 'Employee age (16-75 employment eligibility)',

    -- Sensitive data (encrypted)
    ssn_encrypted STRING NOT NULL COMMENT 'Encrypted Social Security Number',

    -- Employment data
    hourly_rate_cents INT NOT NULL COMMENT 'Hourly wage rate in cents',
    gender STRING NOT NULL COMMENT 'Gender identification from approved values',
    email STRING NOT NULL COMMENT 'Company email address (validated)',

    -- Processing metadata
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When record was received',
    validation_timestamp TIMESTAMP NOT NULL COMMENT 'When validation completed',
    processing_latency_ms BIGINT NOT NULL COMMENT 'Validation processing time',

    -- Data quality information
    validation_rules_version STRING NOT NULL COMMENT 'Version of validation rules applied',
    quality_score DOUBLE NOT NULL COMMENT 'Overall data quality score (0.0-1.0)',
    compliance_flags ARRAY<STRING> COMMENT 'Any compliance flags or notes',

    -- Source system tracking
    source_system STRING NOT NULL COMMENT 'Originating HR system',
    pipeline_version STRING NOT NULL COMMENT 'Version of data quality pipeline',

    -- Partitioning columns (derived from ingestion_timestamp)
    year INT NOT NULL COMMENT 'Year for partitioning',
    month INT NOT NULL COMMENT 'Month for partitioning',
    day INT NOT NULL COMMENT 'Day for partitioning',
    hour INT NOT NULL COMMENT 'Hour for partitioning'
)
USING ICEBERG
PARTITIONED BY (year, month, day, hour)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.compression-codec' = 'gzip',
    'write.target-file-size-bytes' = '134217728', -- 128MB
    'write.upsert.enabled' = 'false',
    'history.expire.max-snapshot-age-ms' = '604800000', -- 7 days
    'write.object-storage.enabled' = 'true',
    'write.object-storage.path' = 's3a://payroll-data-lake/tables/validated_employees'
);

-- Create Z-order clustering for better query performance
ALTER TABLE payroll_data_lake.validated_employees
WRITE ORDERED BY (employee_id, validation_timestamp);

-- Create compliance audit table for regulatory requirements
CREATE TABLE payroll_data_lake.compliance_audit_log (
    audit_id STRING NOT NULL,
    employee_id INT NOT NULL,
    audit_timestamp TIMESTAMP NOT NULL,
    audit_type STRING NOT NULL COMMENT 'PII_ACCESS, VALIDATION_DECISION, ENCRYPTION_EVENT',
    user_id STRING NOT NULL COMMENT 'Identity of user/system accessing data',
    operation_details STRING NOT NULL,
    pii_fields_accessed ARRAY<STRING> COMMENT 'Which PII fields were accessed',
    compliance_status STRING NOT NULL COMMENT 'COMPLIANT, VIOLATION, REVIEW_REQUIRED',
    retention_expires TIMESTAMP NOT NULL COMMENT 'When audit record expires'
)
USING ICEBERG
PARTITIONED BY (days(audit_timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'history.expire.max-snapshot-age-ms' = '220752000000' -- 7 years for compliance
);

-- Create data quality metrics summary table
CREATE TABLE payroll_data_lake.quality_metrics_summary (
    metric_window_start TIMESTAMP NOT NULL,
    metric_window_end TIMESTAMP NOT NULL,
    total_records_processed BIGINT NOT NULL,
    valid_records_count BIGINT NOT NULL,
    invalid_records_count BIGINT NOT NULL,
    compliance_violations_count BIGINT NOT NULL,
    average_validation_latency_ms DOUBLE NOT NULL,
    records_per_second DOUBLE NOT NULL,
    rule_performance_metrics MAP<STRING, STRUCT<
        executions: BIGINT,
        failures: BIGINT,
        avg_execution_time_ms: DOUBLE
    >> NOT NULL COMMENT 'Per-rule performance statistics'
)
USING ICEBERG
PARTITIONED BY (hours(metric_window_start));