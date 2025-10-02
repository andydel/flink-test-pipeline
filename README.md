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
Kafka (Payroll Records) → Flink Pipeline → Iceberg (Valid Records)
                                      ↓
                               Failed Records → HR Workflow Queues
                                      ↓
                               Audit Logs → Long-term Storage
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
# Start all services (Kafka, Schema Registry, Postgres, Flink)
docker-compose up -d

# Wait for services to be ready (about 60 seconds)
docker-compose logs -f payroll-flink-jobmanager
```

### 4. Deploy the Pipeline

```bash
# Copy the JAR to Flink container
docker cp target/payroll-pipeline-1.0.0.jar payroll-flink-jobmanager:/opt/flink/lib/

# Deploy the pipeline
docker-compose exec payroll-flink-jobmanager flink run \
  -c com.flinkpipeline.payroll.PayrollDataQualityPipeline \
  /opt/flink/lib/payroll-pipeline-1.0.0.jar \
  --kafka.bootstrap.servers=kafka:29092 \
  --kafka.topic=payroll-employees \
  --schema.registry.url=http://schema-registry:8081 \
  --iceberg.catalog.uri=jdbc:postgresql://postgres:5432/icebergdb \
  --security.encryption.enabled=true
```

### 5. Send Test Data

```bash
# Start the data generator (optional profile)
docker-compose --profile testing up payroll-data-generator

# Or manually send sample payroll records
docker-compose exec payroll-kafka kafka-console-producer \
  --broker-list kafka:29092 \
  --topic payroll-employees
```

### 6. Monitor the Pipeline

```bash
# Check Flink Web UI
open http://localhost:8080

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

- **Flink Cluster**: JobManager and TaskManager for stream processing (port 8080)
- **Apache Kafka**: Message broker with Zookeeper (port 9092)
- **Confluent Schema Registry**: Avro schema management (port 8081)
- **PostgreSQL**: Iceberg catalog and audit log storage (port 5432)
- **MinIO**: S3-compatible storage for Iceberg data files (ports 9000, 9001)
- **LocalStack**: AWS S3 simulation for development (port 4566)
- **Kafka UI**: Web interface for Kafka debugging (port 8082)
- **Prometheus**: Metrics collection (port 9090, optional profile)
- **Grafana**: Monitoring dashboards (port 3000, optional profile)
- **Data Generator**: Test data generation (optional profile)

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=payroll-employees
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Security Configuration
SECURITY_ENCRYPTION_ENABLED=true
SECURITY_RBAC_ENABLED=true
PII_ENCRYPTION_KEY_ID=payroll-pii-key

# Iceberg Configuration
ICEBERG_CATALOG_URI=jdbc:postgresql://postgres:5432/icebergdb
ICEBERG_WAREHOUSE_PATH=s3a://payroll-data-lake/warehouse

# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_DEFAULT_BUCKETS=payroll-data-lake

# Monitoring
METRICS_ENABLED=true
HEALTH_CHECK_PORT=8080
```

### Pipeline Configuration

Edit `src/main/resources/pipeline.conf`:

```hocon
payroll-pipeline {
  validation {
    rules {
      ssn-format.enabled = true
      age-range.min = 16
      age-range.max = 75
      minimum-wage.cents = 725  # $7.25/hour federal minimum
    }

    duplicate-detection {
      window-size = "PT10M"  # 10 minutes
      similarity-threshold = 0.8
    }
  }

  security {
    encryption {
      algorithm = "AES/GCM/NoPadding"
      key-size = 256
    }

    rbac {
      roles = ["payroll_processor", "hr_manager", "compliance_officer"]
    }
  }

  error-handling {
    max-retries = 3
    backoff-strategy = "exponential"
    circuit-breaker.failure-threshold = 10
  }
}
```

## Development Setup

### Local Development without Docker

1. **Install Dependencies**:
   ```bash
   # Start local Kafka and PostgreSQL
   brew install kafka postgresql
   brew services start kafka
   brew services start postgresql

   # Install Flink locally
   wget https://downloads.apache.org/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
   tar -xzf flink-1.18.0-bin-scala_2.12.tgz
   ```

2. **Build the Project**:
   ```bash
   mvn clean package
   ```

3. **Run Tests**:
   ```bash
   # Unit tests
   mvn test

   # Integration tests (requires Docker for TestContainers)
   mvn verify -P integration-tests
   ```

4. **Start the Pipeline**:
   ```bash
   # Start Flink cluster
   ./flink-1.18.0/bin/start-cluster.sh

   # Deploy pipeline
   ./flink-1.18.0/bin/flink run target/payroll-pipeline-1.0.0.jar
   ```

## Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
```bash
# Requires Docker for TestContainers
mvn verify -P integration-tests
```

### Load Testing
```bash
# Generate 10,000 test records
docker-compose exec flink-jobmanager java -cp /opt/flink/usrlib/payroll-pipeline.jar \
  com.flinkpipeline.payroll.integration.TestDataGenerator \
  --count=10000 --output=/tmp/load-test-data.json

# Send load test data
docker-compose exec kafka kafka-console-producer \
  --broker-list kafka:9092 \
  --topic payroll-employees < /tmp/load-test-data.json
```

## Monitoring & Operations

### Health Checks

```bash
# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe
curl http://localhost:8080/health/ready

# Startup probe
curl http://localhost:8080/health/startup
```

### Metrics

Key metrics exposed at `/metrics`:

- `payroll.records.processed.total`: Total records processed
- `payroll.records.valid.rate`: Valid record processing rate
- `payroll.records.failed.total`: Failed record count
- `payroll.validation.latency.p99`: 99th percentile validation latency
- `payroll.security.pii.operations.total`: PII access operations
- `payroll.compliance.violations.total`: Compliance violations detected

### Logs

```bash
# Pipeline logs
docker-compose logs -f flink-taskmanager

# Audit logs (compliance)
docker-compose exec postgres psql -U postgres -d icebergdb \
  -c "SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 10;"
```

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

All operations are logged with tamper-evident features:

```sql
-- View audit trail
SELECT operation_type, user_id, resource_id, timestamp, checksum
FROM audit_logs
WHERE pii_accessed = true
ORDER BY timestamp DESC;
```

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
   # Check Iceberg catalog
   docker-compose exec postgres psql -U postgres -d icebergdb -c "\dt"

   # Verify Minio storage
   docker-compose logs minio
   ```

4. **Performance Issues**:
   ```bash
   # Check resource utilization
   docker-compose exec flink-taskmanager top

   # View Flink metrics
   curl http://localhost:8081/jobs
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

See `k8s/` directory for Kubernetes manifests:

```bash
# Deploy to Kubernetes
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml
```

### Scaling

The pipeline supports horizontal scaling:

```bash
# Scale TaskManagers
kubectl scale deployment flink-taskmanager --replicas=5

# Auto-scaling based on CPU/memory
kubectl apply -f k8s/hpa.yaml
```

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