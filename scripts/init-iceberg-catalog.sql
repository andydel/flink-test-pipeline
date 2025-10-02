-- Initialize Iceberg catalog tables in PostgreSQL
-- This script sets up the necessary tables for Apache Iceberg catalog

-- Create the Iceberg catalog schema
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;

-- Create the catalog metadata table
CREATE TABLE IF NOT EXISTS iceberg_catalog.catalog_metadata (
    catalog_name VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value TEXT,
    PRIMARY KEY (catalog_name, property_key)
);

-- Create the namespace metadata table
CREATE TABLE IF NOT EXISTS iceberg_catalog.namespace_metadata (
    catalog_name VARCHAR(255) NOT NULL,
    namespace_name VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value TEXT,
    PRIMARY KEY (catalog_name, namespace_name, property_key)
);

-- Create the table metadata table
CREATE TABLE IF NOT EXISTS iceberg_catalog.table_metadata (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location TEXT NOT NULL,
    previous_metadata_location TEXT,
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

-- Insert default catalog properties
INSERT INTO iceberg_catalog.catalog_metadata (catalog_name, property_key, property_value)
VALUES
    ('payroll_catalog', 'catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog'),
    ('payroll_catalog', 'uri', 'jdbc:postgresql://postgres:5432/icebergdb'),
    ('payroll_catalog', 'warehouse', 's3a://payroll-data-lake/warehouse'),
    ('payroll_catalog', 'io-impl', 'org.apache.iceberg.aws.s3.S3FileIO'),
    ('payroll_catalog', 's3.endpoint', 'http://minio:9000'),
    ('payroll_catalog', 's3.access-key-id', 'admin'),
    ('payroll_catalog', 's3.secret-access-key', 'password'),
    ('payroll_catalog', 's3.path-style-access', 'true')
ON CONFLICT (catalog_name, property_key) DO NOTHING;

-- Create payroll namespace
INSERT INTO iceberg_catalog.namespace_metadata (catalog_name, namespace_name, property_key, property_value)
VALUES
    ('payroll_catalog', 'payroll', 'location', 's3a://payroll-data-lake/warehouse/payroll'),
    ('payroll_catalog', 'payroll', 'description', 'Payroll data namespace for employee records and audit logs'),
    ('payroll_catalog', 'payroll', 'owner', 'payroll-system')
ON CONFLICT (catalog_name, namespace_name, property_key) DO NOTHING;

-- Create audit namespace for compliance logs
INSERT INTO iceberg_catalog.namespace_metadata (catalog_name, namespace_name, property_key, property_value)
VALUES
    ('payroll_catalog', 'audit', 'location', 's3a://payroll-data-lake/warehouse/audit'),
    ('payroll_catalog', 'audit', 'description', 'Audit logs namespace for compliance and security tracking'),
    ('payroll_catalog', 'audit', 'owner', 'compliance-team'),
    ('payroll_catalog', 'audit', 'retention.years', '7')
ON CONFLICT (catalog_name, namespace_name, property_key) DO NOTHING;

-- Create error namespace for failed records
INSERT INTO iceberg_catalog.namespace_metadata (catalog_name, namespace_name, property_key, property_value)
VALUES
    ('payroll_catalog', 'error', 'location', 's3a://payroll-data-lake/warehouse/error'),
    ('payroll_catalog', 'error', 'description', 'Error namespace for failed payroll records'),
    ('payroll_catalog', 'error', 'owner', 'payroll-system')
ON CONFLICT (catalog_name, namespace_name, property_key) DO NOTHING;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON SCHEMA iceberg_catalog TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg_catalog TO postgres;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_table_metadata_catalog_namespace
    ON iceberg_catalog.table_metadata (catalog_name, table_namespace);

CREATE INDEX IF NOT EXISTS idx_namespace_metadata_catalog
    ON iceberg_catalog.namespace_metadata (catalog_name, namespace_name);

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Iceberg catalog initialization completed successfully';
    RAISE NOTICE 'Created catalog: payroll_catalog';
    RAISE NOTICE 'Created namespaces: payroll, audit, error';
END $$;