-- Iceberg table schema for valid events
-- Partitioned by year/month/day/hour based on insertion time

CREATE TABLE data_quality.valid_events (
    -- Event identification
    id STRING NOT NULL COMMENT 'Unique event identifier (UUID)',
    event_type STRING NOT NULL COMMENT 'Type of event for categorization',

    -- Temporal information
    event_timestamp TIMESTAMP NOT NULL COMMENT 'Original event creation time',
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When event was ingested into pipeline',
    processing_timestamp TIMESTAMP NOT NULL COMMENT 'When event validation completed',

    -- Event data
    payload MAP<STRING, STRING> COMMENT 'Event data content as key-value pairs',
    schema_version STRING NOT NULL COMMENT 'Schema version for compatibility',

    -- Metadata
    source_system STRING NOT NULL COMMENT 'Originating system or service',
    correlation_id STRING COMMENT 'For distributed tracing',
    kafka_partition INT NOT NULL COMMENT 'Original Kafka partition',
    kafka_offset BIGINT NOT NULL COMMENT 'Original Kafka offset',

    -- Quality metrics
    validation_latency_ms BIGINT NOT NULL COMMENT 'Time taken for validation in milliseconds',
    rules_applied ARRAY<STRING> NOT NULL COMMENT 'List of quality rules that were applied',

    -- Partitioning columns (computed from ingestion_timestamp)
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
    'write.object-storage.path' = 's3a://data-lake/tables/valid_events'
);

-- Create Z-order clustering for better query performance
ALTER TABLE data_quality.valid_events
WRITE ORDERED BY (event_type, source_system);

-- Create table for metadata and lineage tracking
CREATE TABLE data_quality.event_lineage (
    event_id STRING NOT NULL,
    pipeline_version STRING NOT NULL,
    processing_node STRING NOT NULL,
    validation_rules_version STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
USING ICEBERG
PARTITIONED BY (days(created_at));