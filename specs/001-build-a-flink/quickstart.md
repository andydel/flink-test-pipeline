# Quickstart: Payroll Data Quality Pipeline

## Overview
This guide validates the complete payroll data quality pipeline functionality from employee record ingestion through validation to output routing, with emphasis on HR workflow integration and compliance requirements.

## Prerequisites
- Docker and Docker Compose installed
- Java 17+ for local development
- Access to S3 bucket for Iceberg tables
- Kafka cluster (or use provided docker-compose)
- Payroll employee schema available in `avro/input.avro`

## Quick Setup

### 1. Environment Setup
```bash
# Clone and setup project
git clone <repository>
cd flink-pipeline

# Start local infrastructure
docker-compose up -d kafka zookeeper schema-registry localstack

# Wait for services to be ready (30-60 seconds)
./scripts/wait-for-services.sh
```

### 2. Configure Payroll Pipeline
```bash
# Copy example configuration
cp config/payroll-pipeline.example.properties config/payroll-pipeline.properties

# Edit configuration for your environment
# - Kafka brokers and payroll topics
# - S3 credentials and payroll data bucket
# - Schema registry URL
# - PII encryption keys
vim config/payroll-pipeline.properties
```

### 3. Deploy Payroll Data Quality Pipeline
```bash
# Build application with payroll validation rules
./mvnw clean package -P payroll-profile

# Deploy to local Flink cluster
docker-compose up -d flink-jobmanager flink-taskmanager

# Submit payroll pipeline job
./scripts/submit-payroll-job.sh
```

## Payroll Validation Scenarios

### Scenario 1: Valid Employee Record Processing
**Objective**: Verify valid payroll records are processed and stored correctly

```bash
# Send valid payroll employee record
./scripts/send-payroll-record.sh valid_employee

# Expected outcomes:
# 1. Record appears in Iceberg payroll table within 30 seconds
# 2. No entries in HR failure topic
# 3. Metrics show 100% validation success rate
# 4. Processing latency < 50ms (payroll requirement)
# 5. PII fields (SSN) properly encrypted in storage

# Verify results
./scripts/verify-payroll-iceberg-data.sh
./scripts/check-hr-failure-topic.sh
./scripts/check-compliance-audit-log.sh
```

**Success Criteria**:
- Employee record stored in correct Iceberg partition (by ingestion hour)
- SSN encrypted and access logged for compliance
- All 8 required payroll fields present and validated
- Processing latency recorded and within 50ms SLA
- Compliance audit trail created

### Scenario 2: Invalid SSN Format Handling
**Objective**: Verify SSN format validation and HR-friendly error messages

```bash
# Send employee record with invalid SSN format
./scripts/send-payroll-record.sh invalid_ssn_format

# Expected outcomes:
# 1. Record appears in HR failure topic within 10 seconds
# 2. Error message: "Invalid SSN format - must be XXX-XX-XXXX"
# 3. Suggested correction provided for HR team
# 4. No data stored in main Iceberg table
# 5. Compliance violation flagged and logged

# Verify HR correction workflow
./scripts/verify-hr-failure-topic.sh --filter="ssn"
./scripts/check-payroll-iceberg-table.sh --expect-empty
```

**Success Criteria**:
- Failed record contains original employee data
- Clear, HR-friendly error message provided
- Suggested correction guidance included
- Compliance violation properly categorized
- HR workflow ID assigned for tracking

### Scenario 3: Age Range Validation
**Objective**: Verify employment eligibility age validation

```bash
# Send employee records with invalid ages
./scripts/send-payroll-record.sh age_too_young   # age: 15
./scripts/send-payroll-record.sh age_too_old     # age: 76

# Expected outcomes:
# 1. Both records rejected for age range violation
# 2. Specific error: "Age {age} is outside employment eligibility range (16-75 years)"
# 3. Regulatory compliance flag set
# 4. Critical priority assigned for HR correction
```

**Success Criteria**:
- Age validation enforces 16-75 range
- Regulatory compliance violation flagged
- Critical priority assigned for legal compliance
- Clear guidance for HR correction provided

### Scenario 4: Hourly Rate Validation
**Objective**: Verify wage range validation against federal requirements

```bash
# Send employee records with invalid hourly rates
./scripts/send-payroll-record.sh hourly_rate_too_low   # $5.00/hour
./scripts/send-payroll-record.sh hourly_rate_too_high  # $200.00/hour

# Expected outcomes:
# 1. Both records rejected for hourly rate violations
# 2. Error messages reference federal minimum wage requirements
# 3. Regulatory compliance flags set
# 4. Appropriate correction guidance provided
```

**Success Criteria**:
- Hourly rate validation enforces $7.25-$150.00 range
- Federal minimum wage compliance verified
- Executive compensation cap enforced
- Regulatory violation properly categorized

### Scenario 5: Duplicate Employee Detection
**Objective**: Verify duplicate detection across employee ID, SSN, and email

```bash
# Send initial valid employee record
./scripts/send-payroll-record.sh employee_1001

# Send duplicate records with same employee_id, SSN, or email
./scripts/send-payroll-record.sh duplicate_employee_id
./scripts/send-payroll-record.sh duplicate_ssn
./scripts/send-payroll-record.sh duplicate_email

# Expected outcomes:
# 1. First record processed successfully
# 2. Duplicate records flagged and routed to failure topic
# 3. Clear identification of which field caused duplication
# 4. HR workflow for investigating potential data entry errors
```

**Success Criteria**:
- 1-hour duplicate detection window enforced
- Multi-field duplicate detection (ID, SSN, email)
- Clear indication of duplicate field
- HR investigation workflow triggered

### Scenario 6: Mixed Load with PII Compliance
**Objective**: Verify pipeline handles mixed valid/invalid records while maintaining PII compliance

```bash
# Generate mixed payroll record stream (70% valid, 30% invalid)
./scripts/payroll-load-test.sh --duration=60s --rate=1000/s --mix=70:30

# Monitor compliance during test
./scripts/monitor-pii-compliance.sh
./scripts/monitor-payroll-pipeline.sh

# Expected outcomes:
# 1. Consistent throughput throughout test
# 2. Stable latency (p95 < 50ms for payroll requirement)
# 3. No PII data leakage or compliance violations
# 4. Proper routing percentages maintained
# 5. Real-time compliance audit trail
```

**Success Criteria**:
- Throughput remains stable under payroll processing load
- Latency stays within 50ms payroll requirement
- PII encryption and access logging maintained
- Error rates match expected distribution
- Compliance audit trail complete and accurate

### Scenario 7: HR Correction Workflow Integration
**Objective**: Verify integration with HR correction processes

```bash
# Start pipeline with normal payroll processing
./scripts/payroll-load-test.sh --duration=300s --rate=500/s &

# Inject various validation failures
./scripts/inject-payroll-errors.sh --error-types=all

# Expected outcomes:
# 1. Failed records categorized by error type and severity
# 2. HR workflow IDs assigned for tracking
# 3. Estimated correction times provided
# 4. Priority levels set based on compliance impact
# 5. Integration with HR ticketing system
```

**Success Criteria**:
- Error categorization by HR workflow priority
- Correction time estimates provided
- HR ticketing system integration working
- Compliance violations escalated appropriately

### Scenario 8: Compliance Audit and Reporting
**Objective**: Verify comprehensive audit trail for regulatory compliance

```bash
# Process various payroll records with PII access
./scripts/send-payroll-record.sh valid_with_ssn_access
./scripts/send-payroll-record.sh compliance_violation

# Generate compliance reports
./scripts/generate-compliance-report.sh --period=today

# Expected outcomes:
# 1. Complete audit trail for all PII access
# 2. User identification and timestamp logging
# 3. Compliance violation detection and reporting
# 4. Regulatory report generation capability
```

**Success Criteria**:
- 100% audit coverage for PII access
- User identification for all operations
- Compliance violation detection and escalation
- Regulatory report generation functional

## Monitoring and Observability

### Payroll-Specific Metrics Dashboard
```bash
# Open payroll monitoring dashboard
open http://localhost:3000/dashboard/payroll-pipeline

# Key payroll metrics to verify:
# - Employee records per second (input/valid/invalid)
# - Validation success/failure rates by rule type
# - Processing latency percentiles (50ms SLA)
# - PII access and encryption metrics
# - HR correction workflow efficiency
# - Compliance violation trends
```

### Health Checks for Payroll Processing
```bash
# Payroll pipeline health
curl http://localhost:8081/health/payroll

# PII compliance status
curl http://localhost:8081/health/compliance

# HR workflow integration status
curl http://localhost:8081/health/hr-integration

# Kafka payroll topics lag monitoring
./scripts/check-payroll-consumer-lag.sh
```

### Payroll Data Quality Analysis
```bash
# View payroll pipeline logs
docker logs flink-taskmanager | grep payroll

# Search for validation errors by type
./scripts/search-payroll-logs.sh "validation|compliance|pii"

# Trace specific employee record processing
./scripts/trace-employee-record.sh <employee-id>

# Generate data quality report
./scripts/payroll-quality-report.sh --period=week
```

## Troubleshooting

### Common Payroll Processing Issues

**Pipeline not processing payroll records**:
- Check Kafka payroll topics connectivity: `./scripts/test-payroll-kafka.sh`
- Verify S3 payroll bucket permissions: `./scripts/test-payroll-s3.sh`
- Review Flink logs for payroll-specific errors: `docker logs flink-jobmanager | grep payroll`

**PII encryption/compliance issues**:
- Verify encryption key configuration: `./scripts/test-pii-encryption.sh`
- Check compliance audit log: `./scripts/check-compliance-audit.sh`
- Review access control settings: `./scripts/verify-pii-access-controls.sh`

**High validation latency (>50ms)**:
- Check resource allocation: `./scripts/check-payroll-resources.sh`
- Review validation rule performance: `./scripts/analyze-rule-performance.sh`
- Analyze PII encryption overhead: `./scripts/profile-pii-operations.sh`

**HR workflow integration failures**:
- Test HR topic connectivity: `./scripts/test-hr-topics.sh`
- Verify error message formatting: `./scripts/test-hr-error-format.sh`
- Check HR system integration: `./scripts/test-hr-integration.sh`

**Validation rule issues**:
- Test individual payroll rules: `./scripts/test-payroll-rules.sh`
- Verify rule configuration: `./scripts/validate-rule-config.sh`
- Check rule execution performance: `./scripts/profile-rule-execution.sh`

## Performance Baselines

### Expected Payroll Processing Performance
- **Throughput**: 10,000+ employee records/second
- **Latency**: <50ms p95 for payroll validation (constitutional requirement)
- **Memory**: <2GB heap per task manager
- **CPU**: <70% utilization at max payroll processing throughput
- **Recovery**: <2 minutes from checkpoint during payroll cycles

### PII and Compliance Performance
- **Encryption Overhead**: <10ms additional latency for SSN encryption
- **Audit Logging**: <5ms additional latency for compliance logging
- **Access Control**: <1ms overhead for PII access validation
- **Compliance Reporting**: Daily reports generated within 15 minutes

### HR Workflow Integration Performance
- **Error Categorization**: <5ms for validation result classification
- **Workflow Assignment**: <10ms for HR ticket creation
- **Correction Tracking**: Real-time status updates
- **Priority Assignment**: Automatic based on compliance impact

## Cleanup
```bash
# Stop all payroll processing services
docker-compose down

# Clean up payroll test data
./scripts/cleanup-payroll-test-data.sh

# Remove Docker volumes (optional)
docker-compose down -v

# Clean up S3 payroll test buckets
./scripts/cleanup-payroll-s3-data.sh
```