# Flink Payroll Data Quality Pipeline

A production-ready Apache Flink streaming pipeline for real-time payroll data quality validation, compliance checking, and audit logging with comprehensive PII security controls.

## Features

- **Real-time Data Quality Validation**: Configurable validation rules for payroll data including federal compliance checks
- **PII Security**: AES-256-GCM encryption for sensitive data (SSN, email) with role-based access control
- **Compliance & Audit**: Comprehensive audit logging with tamper-evident features for regulatory compliance
- **Error Handling**: Advanced error handling with circuit breakers, retry logic, and dead letter queues
- **Duplicate Detection**: Windowed duplicate detection with configurable similarity algorithms
- **Monitoring**: Production-grade metrics and health checks with Kubernetes readiness
- **Auto-scaling**: Business-aware resource management based on payroll processing patterns

## Architecture

```
Kafka (Payroll Records) → Flink Pipeline → Kafka (Validated Records)
                                      ↓
                               Failed Records → HR Workflow Queues
                                      ↓
                               Audit Logs → (disabled in this build)
```

### Core Components

- **PayrollValidationOperator**: Main validation orchestrator with federal compliance rules
- **HRWorkflowRoutingOperator**: Intelligent routing for failed records with priority queues
- **PayrollSecurityManager**: Enterprise security with PII encryption and RBAC
- **PayrollDataQualityPipeline**: Main pipeline coordinator with monitoring and graceful shutdown

## Prerequisites

- Docker Desktop
- Java 11+ (for local development)
- Git

## Quick Start with Docker

### 1. Clone the Repository

```bash
git clone git@github.com:andydel/flink-test-pipeline.git
cd flink-test-pipeline
```

### 2. Build the Project

```bash
# Build the payroll pipeline JAR
mvn clean package -DskipTests
```

### 3. Start the Complete Environment

```bash
# Start all services (Kafka, Schema Registry, Iceberg REST catalog, MinIO, Flink)
docker-compose up -d

# Wait for services to be ready (about 60 seconds)
docker-compose logs -f flink-jobmanager
```

### 4. Deploy the Pipeline

```bash
# Copy the JAR to Flink container
docker cp target/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar payroll-flink-jobmanager:/opt/flink/lib/

# Deploy the pipeline
docker-compose exec flink-jobmanager flink run \
  -c com.flinkpipeline.payroll.PayrollDataQualityPipeline \
  /opt/flink/lib/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar
```

The jar ships with `src/main/resources/application.properties` baked in, which already
points at the docker-compose services (Kafka on `kafka:29092`, the Iceberg REST catalog
at `http://rest:8181`, MinIO as the S3 endpoint, etc.), so no extra flags are required
for the bundled environment. Any of those keys can still be overridden per run, e.g.:

```bash
docker-compose exec flink-jobmanager flink run \
  -c com.flinkpipeline.payroll.PayrollDataQualityPipeline \
  /opt/flink/lib/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar \
  --payroll.kafka.topics=payroll-employees \
  --payroll.security.pii.encryption.enabled=true
```

### 5. Send Test Data

```bash
# Start the data generator (optional profile)
docker-compose --profile testing up payroll-data-generator

# Or manually send sample payroll records
docker-compose exec kafka kafka-console-producer \
  --broker-list kafka:29092 \
  --topic payroll-employees
```

### 6. Monitor the Pipeline

```bash
# Check Flink Web UI (published on 8085, not 8080 - see Docker Compose Services below)
open http://localhost:8085

# Check Kafka UI
open http://localhost:8082

# View MinIO console
open http://localhost:9001

# Start monitoring stack (optional)
docker-compose --profile monitoring up -d
open http://localhost:3000  # Grafana (admin/admin)
open http://localhost:9090  # Prometheus
```

## Docker Compose Services

The `docker-compose.yml` includes:

- **Flink Cluster**: JobManager and TaskManager for stream processing (JobManager UI/REST published on host port **8085**, not 8080 - Flink's container-internal REST port 8080 is already in use by the JobManager process itself)
- **Apache Kafka**: Message broker with Zookeeper (port 9092)
- **Confluent Schema Registry**: Avro schema management (port 8081)
- **Iceberg REST Catalog**: the catalog implementation the pipeline actually talks to (port 8181)
- **MinIO**: S3-compatible storage backing the Iceberg warehouse (ports 9000, 9001)
- **Kafka UI**: Web interface for Kafka debugging (port 8082)
- **Prometheus**: Metrics collection (port 9090, optional `monitoring` profile)
- **Grafana**: Monitoring dashboards (port 3000, optional `monitoring` profile)
- **Data Generator**: Test data generation (optional `testing` profile)
- **Spark/Jupyter**: Iceberg-warehouse exploration environment, not required by the pipeline itself but starts with the rest of the stack (ports 8888, 8079)

## Configuration

Configuration is loaded, in increasing order of precedence, from: built-in defaults →
`src/main/resources/application.properties` (bundled in the jar) → a `PAYROLL_*`
environment variable → an external file passed via `-Dpayroll.config.file=<path>` →
a `--key=value` command-line argument. See `PayrollPipelineConfig` for the full set of
supported keys - every key is `payroll.*`-prefixed except for the AWS/S3 (`aws.*`) and
`kafka.topic.payroll.valid` keys.

### Environment Variables

Any environment variable prefixed `PAYROLL_` is mapped onto the matching
`payroll.*` configuration key (`_` becomes `.`, lowercased). For example:

```bash
# Overrides payroll.kafka.bootstrap.servers
PAYROLL_KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Overrides payroll.iceberg.rest.uri
PAYROLL_ICEBERG_REST_URI=http://rest:8181

# Overrides payroll.security.pii.encryption.enabled
PAYROLL_SECURITY_PII_ENCRYPTION_ENABLED=true

# Overrides payroll.health.port
PAYROLL_HEALTH_PORT=8090
```

AWS/S3 credentials for the Iceberg catalog are read from the non-prefixed
`aws.access.key.id` / `aws.secret.access.key` / `aws.region` / `aws.s3.endpoint` /
`aws.s3.path-style-access` keys, so they can only be overridden via
`application.properties`, `-Dpayroll.config.file`, or `--aws.access.key.id=...` style
CLI flags - not via environment variables.

### Pipeline Configuration

Edit `src/main/resources/application.properties` (plain Java properties format, not
HOCON) and rebuild, or override individual keys per-run as shown above. Quality rules
themselves are defined in code in `PayrollPipelineConfig.getDefaultQualityRules()`
(SSN format, age range, wage compliance, email format) rather than in a config file.

## Development Setup

### Local Development without Docker

1. **Install Dependencies**:
   ```bash
   # Start local Kafka
   brew install kafka
   brew services start kafka

   # Install Flink locally
   wget https://downloads.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
   tar -xzf flink-1.18.1-bin-scala_2.12.tgz
   ```

2. **Build the Project**:
   ```bash
   mvn clean package
   ```

3. **Run Tests**:
   ```bash
   # All tests (unit + testcontainers-based integration tests under src/test/java,
   # requires Docker - there is no separate Maven profile splitting the two today)
   mvn test
   ```

4. **Start the Pipeline**:
   ```bash
   # Start Flink cluster
   ./flink-1.18.1/bin/start-cluster.sh

   # Deploy pipeline
   ./flink-1.18.1/bin/flink run \
     -c com.flinkpipeline.payroll.PayrollDataQualityPipeline \
     target/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar
   ```

## Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
```bash
# testcontainers-based integration tests live under src/test/java too and run as
# part of `mvn test` - there is no separate Maven profile for them today.
mvn test
```

### Load Testing
```bash
# Generate 10,000 test records
docker-compose exec flink-jobmanager java -cp /opt/flink/lib/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar \
  com.flinkpipeline.payroll.integration.TestDataGenerator \
  --count=10000 --output=/tmp/load-test-data.json

# Send load test data
docker-compose exec kafka kafka-console-producer \
  --broker-list kafka:29092 \
  --topic payroll-employees < /tmp/load-test-data.json
```

## Monitoring & Operations

### Health Checks

The pipeline's `HealthCheckServer` starts inside whichever process runs
`PayrollDataQualityPipeline.main()` - the JobManager container, when deployed via
`flink run` per the Quick Start above - listening on `payroll.health.port` (default
`8090`, published as `8085:8080`/`8090:8090` on the `flink-jobmanager` service; see
`docker-compose.yml`). It is independent of Flink's own REST API on 8085.

```bash
# Liveness probe
curl http://localhost:8090/health/live

# Readiness probe
curl http://localhost:8090/health/ready

# Startup probe
curl http://localhost:8090/health/startup
```

### Metrics

`MetricsCollector` currently only logs metrics via SLF4J
(`payroll.metrics.reporter=slf4j`); the Prometheus code path
(`MetricsCollector.reportToPrometheus`) is an explicit placeholder that does not expose
an HTTP endpoint yet, so there is no `/metrics` endpoint to scrape today.
`docker/prometheus.yml` self-scrapes Prometheus only until this is wired up (see the
comment in that file for what's needed to add a real Flink metrics target).

### Logs

```bash
# Pipeline logs
docker-compose logs -f flink-taskmanager
```

Note: compliance audit logs are written via the `AuditLogIcebergSinkConnector` into
an Iceberg table (through the REST catalog / MinIO), not into a separate database -
there is no `audit_logs` SQL table to query directly.

## Security

### PII Encryption

All sensitive fields (SSN, email) are encrypted at rest using AES-256-GCM:

```java
// Automatic encryption in pipeline
PayrollEmployee employee = PayrollEmployee.builder()
    .ssn("123-45-6789")  // Automatically encrypted
    .email("john@company.com")  // Automatically encrypted
    .build();
```

### Access Control

Role-based access control with principle of least privilege:

- **payroll_processor**: Read/process payroll data
- **hr_manager**: Access failed records and routing
- **compliance_officer**: Full audit log access

### Audit Trail

Audit records (`ComplianceAuditLog`) are written by `AuditLogIcebergSinkConnector`
into an Iceberg table via the REST catalog, queryable with any Iceberg-aware engine
(e.g. the optional `spark-iceberg` notebook environment - see the `notebooks` profile
above) rather than with a direct SQL client against a database.

## Compliance

The pipeline implements federal employment law compliance:

- **SSN Validation**: Format validation with area/group/serial checks
- **Age Verification**: Employment age range 16-75 years
- **Wage Compliance**: Federal minimum wage validation ($7.25/hour)
- **Data Retention**: 7-year audit log retention for payroll records
- **PII Protection**: GDPR/CCPA compliant encryption and access controls

## Troubleshooting

### Common Issues

1. **Pipeline Not Starting**:
   ```bash
   # Check Flink cluster status
   docker-compose logs flink-jobmanager

   # Verify Kafka connectivity
   docker-compose exec flink-jobmanager nc -zv kafka 9092
   ```

2. **Schema Registry Issues**:
   ```bash
   # Check Schema Registry health
   curl http://localhost:8081/subjects

   # Reset schema cache
   docker-compose restart schema-registry
   ```

3. **Iceberg Table Issues**:
   ```bash
   # Check the Iceberg REST catalog is up
   curl http://localhost:8181/v1/config

   # Verify MinIO storage
   docker-compose logs minio
   ```

4. **Performance Issues**:
   ```bash
   # Check resource utilization
   docker-compose exec flink-taskmanager top

   # View running Flink jobs (REST API is on 8085, not 8081 - that's Schema Registry)
   curl http://localhost:8085/jobs/overview
   ```

### Debug Mode

Enable debug logging:

```bash
# Set debug environment
export FLINK_LOG_LEVEL=DEBUG
export PAYROLL_PIPELINE_DEBUG=true

# Restart with debug
docker-compose down && docker-compose up -d
```

## Production Deployment

### Kubernetes

Kubernetes manifests are not included in this repository yet (there is no `k8s/`
directory). The pipeline JAR and the corrected `docker/Dockerfile` are a reasonable
starting point for building a Flink-on-Kubernetes deployment using the [official
Flink Kubernetes operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/),
but that manifest set does not exist yet - treat this section as a roadmap item, not
a working deployment path.

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-validation-rule`
3. Write tests for your changes
4. Implement your feature
5. Run the test suite: `mvn verify`
6. Commit your changes: `git commit -am 'Add new validation rule'`
7. Push to the branch: `git push origin feature/new-validation-rule`
8. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:

- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review Flink documentation: https://flink.apache.org/

## Roadmap

- [ ] Schema evolution support with Confluent Schema Registry
- [ ] Real-time alerting integration with PagerDuty/Slack
- [ ] Machine learning-based anomaly detection
- [ ] Multi-region disaster recovery
- [ ] Advanced duplicate detection with fuzzy matching
- [ ] Integration with Apache Airflow for batch processing
