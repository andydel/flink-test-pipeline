# Data Model: Payroll Data Quality Pipeline

## Core Entities

### PayrollEmployee
**Purpose**: Represents employee payroll record from Avro schema
**Fields**:
- `employee_id`: Integer - Unique employee identifier (positive integer)
- `first_name`: String - Employee first name (1-50 chars, letters/spaces/hyphens/apostrophes)
- `last_name`: String - Employee last name (1-50 chars, letters/spaces/hyphens/apostrophes)
- `age`: Integer - Employee age (16-75 for employment eligibility)
- `ssn`: String - Social Security Number (XXX-XX-XXXX format, encrypted)
- `hourly_rate`: Integer - Hourly wage rate ($7.25-$150.00 range)
- `gender`: String - Gender identification ("male", "female", "non-binary", "prefer-not-to-say")
- `email`: String - Company email address (validated format and domain)

**Validation Rules**:
- `employee_id` must be positive and unique within 1-hour window
- `first_name` and `last_name` must be non-empty and follow name format rules
- `age` must be within employment eligibility range (16-75)
- `ssn` must follow XXX-XX-XXXX format and not be blacklisted
- `hourly_rate` must be within legal wage range
- `gender` must be from approved enumeration
- `email` must have valid format and approved company domain

**State Transitions**:
- RECEIVED → VALIDATING → (VALID | INVALID)
- VALID → ENCRYPTED_PII → ROUTED_TO_ICEBERG
- INVALID → ERROR_CATEGORIZED → ROUTED_TO_FAILURE_TOPIC

### PayrollValidationResult
**Purpose**: Detailed outcome of payroll record validation
**Fields**:
- `employee_id`: Integer - Reference to original employee record
- `validation_timestamp`: Long - When validation completed
- `overall_status`: ValidationStatus - VALID, INVALID, or COMPLIANCE_VIOLATION
- `field_results`: List<FieldValidationResult> - Results for each field
- `compliance_flags`: List<ComplianceFlag> - PII and regulatory concerns
- `processing_latency_ms`: Long - Validation duration in milliseconds
- `rule_version`: String - Version of validation rules applied

### FieldValidationResult
**Purpose**: Validation outcome for individual payroll fields
**Fields**:
- `field_name`: String - Name of validated field (e.g., "ssn", "age")
- `rule_name`: String - Specific rule that was applied
- `rule_type`: RuleType - FORMAT, RANGE, COMPLIANCE, UNIQUENESS, COMPLETENESS
- `status`: FieldStatus - PASSED, FAILED, WARNING
- `error_message`: String - HR-friendly error description
- `severity`: Severity - CRITICAL, WARNING, INFO
- `suggested_correction`: String - Guidance for HR correction

### PayrollQualityRule
**Purpose**: Configurable validation rules for payroll fields
**Fields**:
- `rule_id`: String - Unique rule identifier (e.g., "DQ-005")
- `rule_name`: String - Human-readable rule name
- `field_name`: String - Target field for validation
- `rule_type`: RuleType - Category of validation rule
- `validation_expression`: String - Rule logic implementation
- `error_template`: String - Template for error messages
- `compliance_level`: ComplianceLevel - REGULATORY, BUSINESS, INFORMATIONAL
- `enabled`: Boolean - Whether rule is currently active
- `cache_duration_ms`: Long - Caching duration for expensive rules

**Rule Types**:
- FORMAT: SSN pattern, email format, name character validation
- RANGE: Age limits, hourly rate bounds, string length limits
- COMPLIANCE: PII encryption, audit trail requirements
- UNIQUENESS: Employee ID duplication, SSN duplication detection
- COMPLETENESS: Required field presence, non-null validation

### FailedPayrollRecord
**Purpose**: Invalid payroll record with comprehensive error details
**Fields**:
- `original_record`: PayrollEmployee - The failed employee record
- `validation_result`: PayrollValidationResult - Detailed failure information
- `failure_timestamp`: Long - When failure was recorded
- `hr_workflow_id`: String - Reference for HR correction tracking
- `correction_priority`: Priority - CRITICAL, HIGH, MEDIUM, LOW
- `estimated_correction_time`: Integer - Minutes for typical correction
- `compliance_risk_level`: RiskLevel - Risk assessment for regulatory issues

### PayrollQualityMetrics
**Purpose**: Real-time processing statistics for payroll data
**Fields**:
- `metric_window_start`: Long - Metrics collection window start
- `metric_window_end`: Long - Metrics collection window end
- `total_records_processed`: Long - Total payroll records processed
- `valid_records_count`: Long - Records that passed all validations
- `invalid_records_count`: Long - Records that failed validation
- `compliance_violations_count`: Long - Records with PII/regulatory issues
- `average_validation_latency_ms`: Double - Mean processing time
- `records_per_second`: Double - Processing throughput
- `rule_performance_metrics`: Map<String, RuleMetrics> - Per-rule statistics

### ComplianceAuditLog
**Purpose**: Immutable audit trail for regulatory compliance
**Fields**:
- `audit_id`: String - Unique audit entry identifier
- `employee_id`: Integer - Reference to employee record
- `audit_timestamp`: Long - When audit event occurred
- `audit_type`: AuditType - PII_ACCESS, VALIDATION_DECISION, ENCRYPTION_EVENT
- `user_id`: String - Identity of user/system accessing data
- `operation_details`: String - Description of operation performed
- `pii_fields_accessed`: List<String> - Which PII fields were accessed
- `compliance_status`: ComplianceStatus - COMPLIANT, VIOLATION, REVIEW_REQUIRED
- `retention_expires`: Long - When audit record expires

## Entity Relationships

```
PayrollEmployee (1) ←→ (1) PayrollValidationResult
PayrollEmployee (1) ←→ (*) FieldValidationResult
PayrollQualityRule (1) ←→ (*) FieldValidationResult
PayrollEmployee (1) ←→ (0..1) FailedPayrollRecord
PayrollValidationResult (1) ←→ (0..1) FailedPayrollRecord
PayrollEmployee (1) ←→ (*) ComplianceAuditLog
PayrollValidationResult (*) ←→ (1) PayrollQualityMetrics
```

## Data Flow - Payroll Processing Pipeline

1. **Ingestion**: PayrollEmployee records arrive from HR systems via Kafka
2. **PII Identification**: SSN and sensitive fields identified for special handling
3. **Validation**: Each record validated against 10 payroll-specific quality rules
4. **Compliance Check**: PII encryption and audit trail requirements verified
5. **Routing**: Based on validation outcome:
   - Valid records → PII encrypted → Iceberg table with time partitioning
   - Invalid records → Failure topic with detailed HR correction guidance
6. **Audit Logging**: All PII access and validation decisions logged
7. **Metrics Collection**: Real-time quality metrics aggregated for dashboards

## Partitioning Strategy

### Iceberg Table Partitioning
- **Primary**: By ingestion time (year/month/day/hour) for query optimization
- **Secondary**: By validation status for quality reporting
- **Z-ordering**: On employee_id and processing_timestamp for lookup performance

### Kafka Topic Partitioning
- **Input Topic**: By employee_id hash for balanced processing
- **Failure Topic**: By error severity for HR workflow routing
- **Audit Topic**: By audit_type for compliance reporting

## Schema Evolution and PII Handling

### Backward Compatibility
- New optional fields can be added without breaking existing consumers
- Field types cannot be changed (especially for financial data like hourly_rate)
- Removal of fields requires deprecation period and migration notice

### PII Encryption Strategy
- SSN fields encrypted with AES-256 before any processing or storage
- Encryption keys managed separately with automatic rotation
- Access to encrypted fields logged in ComplianceAuditLog
- Decryption only performed when necessary for validation rules

### HR Integration Points
- Failure records include correction guidance for each field
- Error severity mapping to HR workflow priority levels
- Integration with existing HR ticketing systems via Kafka connectors
- Correction tracking through hr_workflow_id references

## Compliance and Regulatory Requirements

### Audit Trail Requirements
- Immutable logging of all PII access and validation decisions
- Retention period aligned with payroll record requirements (7 years)
- User identification and timestamp for all operations
- Integration with compliance monitoring and reporting systems

### Data Quality Metrics for Payroll
- Success/failure rates by validation rule type
- Processing latency tracking to meet payroll deadlines
- Compliance violation trending and alerting
- HR correction workflow efficiency metrics