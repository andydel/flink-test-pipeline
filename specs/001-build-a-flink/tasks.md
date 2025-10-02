# Tasks: Payroll Data Quality Pipeline

**Input**: Design documents from `/Users/andy/IdeaProjects/flink-pipeline/flink-pipeline/specs/001-build-a-flink/`
**Prerequisites**: plan.md, research.md, data-model.md, contracts/, quickstart.md

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `src/main/java/`, `src/test/java/` at repository root
- Paths based on Maven conventions with package `com.flinkpipeline.payroll`
- Docker files in `docker/` directory
- Configuration in `src/main/resources/`
- Payroll schemas in `avro/` directory

## Phase 3.1: Setup
- [x] T001 Create Maven project structure with Java 17, Flink 1.18, Avro dependencies in pom.xml
- [x] T002 Setup Docker infrastructure with docker-compose.yml for Kafka, Schema Registry, LocalStack S3, Flink cluster
- [x] T003 [P] Configure code formatting with google-java-format and Checkstyle for payroll package in pom.xml
- [x] T004 [P] Create payroll pipeline configuration template in src/main/resources/payroll-pipeline.properties
- [x] T005 [P] Setup structured logging configuration with compliance audit support in src/main/resources/logback.xml

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**
- [x] T006 [P] Payroll employee schema validation test in src/test/java/com/flinkpipeline/payroll/schemas/PayrollEmployeeSchemaTest.java
- [x] T007 [P] Failed payroll record schema validation test in src/test/java/com/flinkpipeline/payroll/schemas/FailedPayrollRecordSchemaTest.java
- [x] T008 [P] Iceberg payroll table schema contract test in src/test/java/com/flinkpipeline/payroll/iceberg/PayrollTableSchemaTest.java
- [x] T009 [P] Payroll quality rules configuration validation test in src/test/java/com/flinkpipeline/payroll/rules/PayrollRulesConfigTest.java
- [x] T010 [P] SSN format validation rule test in src/test/java/com/flinkpipeline/payroll/validation/SSNValidationTest.java
- [x] T011 [P] Age range employment eligibility test in src/test/java/com/flinkpipeline/payroll/validation/AgeValidationTest.java
- [x] T012 [P] Hourly rate wage compliance test in src/test/java/com/flinkpipeline/payroll/validation/WageValidationTest.java
- [x] T013 [P] Employee duplicate detection test in src/test/java/com/flinkpipeline/payroll/validation/DuplicateDetectionTest.java
- [x] T014 [P] PII encryption and compliance test in src/test/java/com/flinkpipeline/payroll/compliance/PIIEncryptionTest.java
- [x] T015 [P] Valid employee record processing integration test in src/test/java/com/flinkpipeline/payroll/integration/ValidEmployeeProcessingTest.java
- [x] T016 [P] Invalid SSN format handling integration test in src/test/java/com/flinkpipeline/payroll/integration/InvalidSSNHandlingTest.java
- [x] T017 [P] Age range validation integration test in src/test/java/com/flinkpipeline/payroll/integration/AgeRangeValidationTest.java
- [x] T018 [P] Hourly rate validation integration test in src/test/java/com/flinkpipeline/payroll/integration/HourlyRateValidationTest.java
- [x] T019 [P] Duplicate employee detection integration test in src/test/java/com/flinkpipeline/payroll/integration/DuplicateDetectionIntegrationTest.java
- [x] T020 [P] Mixed load with PII compliance integration test in src/test/java/com/flinkpipeline/payroll/integration/MixedLoadComplianceTest.java
- [x] T021 [P] HR correction workflow integration test in src/test/java/com/flinkpipeline/payroll/integration/HRWorkflowIntegrationTest.java
- [x] T022 [P] Compliance audit and reporting integration test in src/test/java/com/flinkpipeline/payroll/integration/ComplianceAuditTest.java

## Phase 3.3: Core Implementation (ONLY after tests are failing)
- [x] T023 [P] PayrollEmployee data model in src/main/java/com/flinkpipeline/payroll/models/PayrollEmployee.java
- [x] T024 [P] PayrollValidationResult data model in src/main/java/com/flinkpipeline/payroll/models/PayrollValidationResult.java
- [x] T025 [P] FieldValidationResult data model in src/main/java/com/flinkpipeline/payroll/models/FieldValidationResult.java
- [ ] T026 [P] PayrollQualityRule data model in src/main/java/com/flinkpipeline/payroll/models/PayrollQualityRule.java
- [ ] T027 [P] FailedPayrollRecord data model in src/main/java/com/flinkpipeline/payroll/models/FailedPayrollRecord.java
- [ ] T028 [P] ComplianceAuditLog data model in src/main/java/com/flinkpipeline/payroll/models/ComplianceAuditLog.java
- [ ] T029 [P] Avro deserializer for payroll records in src/main/java/com/flinkpipeline/payroll/serialization/PayrollAvroDeserializer.java
- [ ] T030 [P] Avro schema manager for schema registry in src/main/java/com/flinkpipeline/payroll/serialization/PayrollSchemaManager.java
- [x] T031 [P] SSN validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/SSNValidationRule.java
- [x] T032 [P] Age range validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/AgeRangeValidationRule.java
- [x] T033 [P] Hourly rate validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/HourlyRateValidationRule.java
- [ ] T034 [P] Name format validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/NameFormatValidationRule.java
- [ ] T035 [P] Email format validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/EmailValidationRule.java
- [ ] T036 [P] Duplicate detection validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/DuplicateDetectionRule.java
- [x] T037 Payroll rule engine implementation in src/main/java/com/flinkpipeline/payroll/validation/PayrollRuleEngine.java
- [ ] T038 Payroll validation operator implementation in src/main/java/com/flinkpipeline/payroll/operators/PayrollValidationOperator.java
- [ ] T039 HR workflow routing operator implementation in src/main/java/com/flinkpipeline/payroll/operators/HRWorkflowRoutingOperator.java
- [ ] T040 [P] Kafka payroll source connector in src/main/java/com/flinkpipeline/payroll/connectors/kafka/PayrollKafkaSource.java
- [ ] T041 [P] Kafka HR failure sink connector in src/main/java/com/flinkpipeline/payroll/connectors/kafka/HRFailureSink.java
- [ ] T042 [P] Iceberg payroll sink connector in src/main/java/com/flinkpipeline/payroll/connectors/iceberg/PayrollIcebergSink.java

## Phase 3.4: Integration
- [ ] T043 [P] PII encryption service implementation in src/main/java/com/flinkpipeline/payroll/compliance/PIIEncryptionService.java
- [ ] T044 [P] Compliance auditor implementation in src/main/java/com/flinkpipeline/payroll/compliance/ComplianceAuditor.java
- [ ] T045 [P] Payroll metrics collector implementation in src/main/java/com/flinkpipeline/payroll/monitoring/PayrollMetricsCollector.java
- [ ] T046 [P] HR workflow integration service in src/main/java/com/flinkpipeline/payroll/hr/HRWorkflowService.java
- [ ] T047 Payroll pipeline configuration management in src/main/java/com/flinkpipeline/payroll/app/PayrollPipelineConfiguration.java
- [ ] T048 Main payroll data quality pipeline application in src/main/java/com/flinkpipeline/payroll/app/PayrollDataQualityPipeline.java
- [ ] T049 Checkpoint and state backend configuration with PII encryption support
- [ ] T050 [P] Payroll health check handler in src/main/java/com/flinkpipeline/payroll/monitoring/PayrollHealthCheckHandler.java
- [ ] T051 [P] Structured logging setup with compliance audit correlation IDs
- [ ] T052 Docker container configuration with payroll pipeline in docker/Dockerfile

## Phase 3.5: Polish
- [ ] T053 [P] Unit tests for payroll validation rules in src/test/java/com/flinkpipeline/payroll/validation/PayrollValidationRulesTest.java
- [ ] T054 [P] Performance and load testing for payroll processing in src/test/java/com/flinkpipeline/payroll/performance/PayrollThroughputTest.java
- [ ] T055 [P] PII compliance validation tests in src/test/java/com/flinkpipeline/payroll/compliance/PIIComplianceTest.java
- [ ] T056 [P] Update payroll pipeline documentation in docs/payroll-pipeline-architecture.md
- [ ] T057 [P] Create payroll deployment scripts in scripts/deploy-payroll-pipeline.sh
- [ ] T058 [P] Create payroll monitoring dashboard configuration
- [ ] T059 Run complete payroll quickstart validation scenarios from quickstart.md
- [ ] T060 Payroll performance tuning and optimization based on 50ms SLA requirements

## Dependencies
- Setup (T001-T005) before all other phases
- Tests (T006-T022) before implementation (T023-T042)
- Data models (T023-T028) before validation rules (T031-T036)
- Serialization (T029-T030) before connectors (T040-T042)
- Validation rules (T031-T036) before rule engine (T037)
- Rule engine (T037) before operators (T038-T039)
- Operators (T038-T039) before pipeline assembly (T047-T048)
- Core implementation before integration (T043-T052)
- Integration before polish (T053-T060)

## Parallel Example
```
# Launch payroll schema and model tests together:
Task: "Payroll employee schema validation test in src/test/java/com/flinkpipeline/payroll/schemas/PayrollEmployeeSchemaTest.java"
Task: "Failed payroll record schema validation test in src/test/java/com/flinkpipeline/payroll/schemas/FailedPayrollRecordSchemaTest.java"
Task: "Iceberg payroll table schema contract test in src/test/java/com/flinkpipeline/payroll/iceberg/PayrollTableSchemaTest.java"
Task: "Payroll quality rules configuration validation test in src/test/java/com/flinkpipeline/payroll/rules/PayrollRulesConfigTest.java"

# Launch payroll validation rule tests together:
Task: "SSN format validation rule test in src/test/java/com/flinkpipeline/payroll/validation/SSNValidationTest.java"
Task: "Age range employment eligibility test in src/test/java/com/flinkpipeline/payroll/validation/AgeValidationTest.java"
Task: "Hourly rate wage compliance test in src/test/java/com/flinkpipeline/payroll/validation/WageValidationTest.java"
Task: "Employee duplicate detection test in src/test/java/com/flinkpipeline/payroll/validation/DuplicateDetectionTest.java"

# Launch payroll data model implementations together:
Task: "PayrollEmployee data model in src/main/java/com/flinkpipeline/payroll/models/PayrollEmployee.java"
Task: "PayrollValidationResult data model in src/main/java/com/flinkpipeline/payroll/models/PayrollValidationResult.java"
Task: "FieldValidationResult data model in src/main/java/com/flinkpipeline/payroll/models/FieldValidationResult.java"
Task: "PayrollQualityRule data model in src/main/java/com/flinkpipeline/payroll/models/PayrollQualityRule.java"

# Launch payroll validation rule implementations together:
Task: "SSN validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/SSNValidationRule.java"
Task: "Age range validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/AgeRangeValidationRule.java"
Task: "Hourly rate validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/HourlyRateValidationRule.java"
Task: "Name format validation rule implementation in src/main/java/com/flinkpipeline/payroll/validation/rules/NameFormatValidationRule.java"
```

## Notes
- [P] tasks = different files, no dependencies between them
- Verify tests fail before implementing (TDD principle)
- Use Testcontainers for integration tests with Kafka and S3
- Implement exactly-once semantics with Flink checkpointing
- Follow constitutional requirements for PII encryption and compliance monitoring
- All payroll validation must meet 50ms latency SLA

## Task Generation Rules Applied
1. **From Contracts**: Each payroll schema file generated contract test task [P]
   - payroll-employee-schema.avsc → T006
   - failed-payroll-record-schema.avsc → T007
   - iceberg-payroll-table-schema.sql → T008
   - payroll-quality-rules-config.json → T009

2. **From Data Model**: Each payroll entity generated model creation task [P]
   - PayrollEmployee → T023, PayrollValidationResult → T024, FieldValidationResult → T025
   - PayrollQualityRule → T026, FailedPayrollRecord → T027, ComplianceAuditLog → T028

3. **From Quickstart Scenarios**: Each payroll scenario generated integration test [P]
   - Valid Employee Record Processing → T015
   - Invalid SSN Format Handling → T016
   - Age Range Validation → T017
   - Hourly Rate Validation → T018
   - Duplicate Employee Detection → T019
   - Mixed Load with PII Compliance → T020
   - HR Correction Workflow Integration → T021
   - Compliance Audit and Reporting → T022

4. **From Payroll Requirements**: Specific validation rules generated [P]
   - 10 payroll quality rules (DQ-001 to DQ-010) → T010-T013, T031-T036
   - PII encryption and compliance → T014, T043-T044
   - HR workflow integration → T046

## Validation Checklist
- [x] All payroll schema contracts have corresponding tests
- [x] All payroll data model entities have implementation tasks
- [x] All tests come before implementation (TDD)
- [x] PII encryption and compliance monitoring tasks included
- [x] HR workflow integration tasks included
- [x] Payroll-specific validation rules included
- [x] Parallel tasks are truly independent
- [x] Each task specifies exact file path
- [x] No task modifies same file as another [P] task