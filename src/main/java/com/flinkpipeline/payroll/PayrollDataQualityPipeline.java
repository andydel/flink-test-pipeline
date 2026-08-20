package com.flinkpipeline.payroll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkpipeline.payroll.config.IcebergConfig;
import com.flinkpipeline.payroll.config.PayrollPipelineConfig;
import com.flinkpipeline.payroll.connectors.AuditLogIcebergSinkConnector;
import com.flinkpipeline.payroll.connectors.PayrollIcebergSinkConnector;
import com.flinkpipeline.payroll.connectors.PayrollKafkaSourceConnector;
import com.flinkpipeline.payroll.models.ComplianceAuditLog;
import com.flinkpipeline.payroll.models.FailedPayrollRecord;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import com.flinkpipeline.payroll.operators.HRWorkflowRoutingOperator;
import com.flinkpipeline.payroll.operators.PayrollValidationOperator;
import com.flinkpipeline.payroll.utils.HealthCheckServer;
import com.flinkpipeline.payroll.utils.MetricsCollector;
import com.flinkpipeline.payroll.utils.PipelineStateManager;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main orchestrator for the Apache Flink payroll data quality pipeline. Coordinates all components
 * including data ingestion, validation, routing, and storage while providing comprehensive
 * monitoring and error handling.
 *
 * <p>Pipeline Architecture: 1. Kafka Source → Payroll Employee Records 2. Validation Operator →
 * Quality Rules & Compliance Checks 3. Valid Records → Iceberg Data Lake 4. Failed Records → HR
 * Workflow Routing 5. Audit Logs → Compliance Storage 6. Metrics & Monitoring → Real-time
 * Dashboards
 *
 * <p>Features: - Exactly-once processing guarantees - Comprehensive error handling and recovery -
 * Real-time metrics and health monitoring - Configurable checkpointing and state management -
 * Auto-scaling and resource optimization - Security and compliance integration
 */
public class PayrollDataQualityPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollDataQualityPipeline.class);

  // Pipeline configuration
  private final PayrollPipelineConfig config;
  private final StreamExecutionEnvironment env;
  private final StreamTableEnvironment tableEnv;

  // Pipeline components
  private PayrollKafkaSourceConnector kafkaSource;
  private PayrollValidationOperator validationOperator;
  private HRWorkflowRoutingOperator routingOperator;
  private PayrollIcebergSinkConnector payrollSink;
  private AuditLogIcebergSinkConnector auditSink;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Infrastructure components
  private MetricsCollector metricsCollector;
  private HealthCheckServer healthCheckServer;
  private PipelineStateManager stateManager;

  // Pipeline state
  private volatile boolean isRunning = false;
  private volatile boolean isHealthy = true;
  private CompletableFuture<JobExecutionResult> executionFuture;

  /** Constructor with configuration */
  public PayrollDataQualityPipeline(PayrollPipelineConfig config) {
    this.config = config;
    this.env = createStreamExecutionEnvironment();
    this.tableEnv = createTableEnvironment();

    LOG.info(
        "Initialized PayrollDataQualityPipeline with config: environment={}, checkpoints={}",
        config.getEnvironment(),
        config.getCheckpointConfig().isEnabled());
  }

  /** Main entry point for the pipeline */
  public static void main(String[] args) throws Exception {
    LOG.info("Starting Payroll Data Quality Pipeline");

    try {
      // Load configuration
      PayrollPipelineConfig config = PayrollPipelineConfig.fromArgs(args);

      // Create and start pipeline
      PayrollDataQualityPipeline pipeline = new PayrollDataQualityPipeline(config);
      pipeline.start();

    } catch (Exception e) {
      LOG.error("Failed to start payroll pipeline", e);
      System.exit(1);
    }
  }

  /** Start the complete pipeline */
  public void start() throws Exception {
    LOG.info("Starting payroll data quality pipeline");

    try {
      // Initialize infrastructure components
      initializeInfrastructure();

      // Build the data pipeline
      buildDataPipeline();

      // Configure monitoring and health checks
      configureMonitoring();

      // Start execution
      startExecution();

      LOG.info("Payroll data quality pipeline started successfully");

    } catch (Exception e) {
      LOG.error("Failed to start pipeline", e);
      shutdown();
      throw e;
    }
  }

  /** Initialize infrastructure components */
  private void initializeInfrastructure() throws Exception {
    LOG.info("Initializing infrastructure components");

    // Initialize metrics collector
    this.metricsCollector = new MetricsCollector(config.getMetricsConfig());
    metricsCollector.start();

    // Initialize health check server
    this.healthCheckServer = new HealthCheckServer(config.getHealthCheckConfig());
    healthCheckServer.start();

    // Initialize state manager
    this.stateManager = new PipelineStateManager(config.getStateConfig());
    stateManager.initialize();

    LOG.info("Infrastructure components initialized");
  }

  /** Build the complete data processing pipeline */
  private void buildDataPipeline() throws Exception {
    LOG.info("Building data processing pipeline");

    // 1. Configure Kafka source
    this.kafkaSource =
        new PayrollKafkaSourceConnector.Builder()
            .bootstrapServers(config.getKafkaConfig().getBootstrapServers())
            .schemaRegistryUrl(config.getKafkaConfig().getSchemaRegistryUrl())
            .topics(config.getKafkaConfig().getTopics())
            .consumerGroupId(config.getKafkaConfig().getConsumerGroup())
            .enableExactlyOnce(config.getKafkaConfig().isExactlyOnceEnabled())
            .build();

    // 2. Create payroll data stream with watermarks
    WatermarkStrategy<PayrollEmployee> watermarkStrategy =
        WatermarkStrategy.<PayrollEmployee>forBoundedOutOfOrderness(Duration.ofMinutes(2))
            .withTimestampAssigner(
                (record, timestamp) ->
                    record.getIngestionTimestamp() != null
                        ? record.getIngestionTimestamp()
                        : System.currentTimeMillis())
            .withIdleness(Duration.ofMinutes(5));

    DataStream<PayrollEmployee> payrollStream =
        kafkaSource.createPayrollDataStream(env, watermarkStrategy);

    // 3. Configure validation operator
    this.validationOperator =
        new PayrollValidationOperator.Builder()
            .withRules(config.getValidationConfig().getQualityRules())
            .enableDuplicateDetection(config.getValidationConfig().isDuplicateDetectionEnabled())
            .enableComplianceAuditing(config.getValidationConfig().isComplianceAuditingEnabled())
            .enablePIIEncryption(config.getSecurityConfig().isPiiEncryptionEnabled())
            .duplicateDetectionWindow(config.getValidationConfig().getDuplicateDetectionWindow())
            .build();

    // 4. Apply validation with side outputs
    SingleOutputStreamOperator<PayrollEmployee> validatedStream =
        payrollStream
            .keyBy(PayrollEmployee::getEmployeeId)
            .process(validationOperator)
            .name("Payroll Validation")
            .uid("payroll-validation-operator");

    // 5. Extract side outputs
    DataStream<FailedPayrollRecord> failedRecords =
        validatedStream.getSideOutput(PayrollValidationOperator.FAILED_RECORDS_TAG);

    DataStream<ComplianceAuditLog> auditLogs =
        validatedStream.getSideOutput(PayrollValidationOperator.AUDIT_LOGS_TAG);

    // 6. Configure HR workflow routing for failed records
    this.routingOperator =
        new HRWorkflowRoutingOperator(
            config.getHrWorkflowConfig().getSlaThreshold(),
            config.getHrWorkflowConfig().getMaxRetryAttempts(),
            config.getHrWorkflowConfig().isLoadBalancingEnabled(),
            config.getHrWorkflowConfig().isEscalationEnabled(),
            config.getHrWorkflowConfig().getEscalationThreshold(),
            config.getHrWorkflowConfig().getHrTeamConfig());

    SingleOutputStreamOperator<FailedPayrollRecord> routedFailedRecords =
        failedRecords
            .keyBy(FailedPayrollRecord::getHrWorkflowId)
            .process(routingOperator)
            .name("HR Workflow Routing")
            .uid("hr-workflow-routing-operator");

    // 7. Merge audit logs from routing
    DataStream<ComplianceAuditLog> allAuditLogs =
        auditLogs.union(
            routedFailedRecords.getSideOutput(HRWorkflowRoutingOperator.AUDIT_LOGS_TAG));

    // 8. Write valid records to Kafka for downstream processing
    sinkValidRecordsToKafka(validatedStream);

    // 9. Configure Iceberg sinks
    this.payrollSink = createPayrollIcebergSink();
    this.auditSink = createAuditLogIcebergSink();

    // 10. Write to Iceberg tables
    payrollSink.createPayrollSink(validatedStream, tableEnv);
    auditSink.createAuditLogSink(allAuditLogs, tableEnv);

    // 11. Add monitoring and metrics
    addMonitoringStreams(validatedStream, failedRecords, auditLogs);

    LOG.info("Data processing pipeline built successfully");
  }

  /** Configure monitoring and metrics collection */
  private void configureMonitoring() {
    LOG.info("Configuring monitoring and metrics");

    // Register health check callbacks
    healthCheckServer.registerHealthCheck(
        "kafka-source",
        () -> kafkaSource != null && kafkaSource.getMetrics().getRecordsConsumed() >= 0);

    healthCheckServer.registerHealthCheck(
        "validation-operator",
        () ->
            validationOperator != null
                && validationOperator.getStatistics().getTotalRecords() >= 0);

    healthCheckServer.registerHealthCheck("pipeline-state", () -> isHealthy);

    // Configure metrics collection
    metricsCollector.registerMetricSource("kafka-source", kafkaSource::getMetrics);
    metricsCollector.registerMetricSource("validation-operator", validationOperator::getStatistics);
    metricsCollector.registerMetricSource("routing-operator", routingOperator::getStatistics);
    metricsCollector.registerMetricSource("payroll-iceberg-sink", payrollSink::getMetrics);
    metricsCollector.registerMetricSource("audit-iceberg-sink", auditSink::getMetrics);

    LOG.info("Monitoring configured successfully");
  }

  /** Start pipeline execution */
  private void startExecution() throws Exception {
    LOG.info("Starting pipeline execution");

    // Mark as running
    isRunning = true;

    // Execute asynchronously
    this.executionFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return env.execute("Payroll Data Quality Pipeline");
              } catch (Exception e) {
                LOG.error("Pipeline execution failed", e);
                isHealthy = false;
                throw new RuntimeException(e);
              }
            });

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

    // Wait for completion or handle graceful shutdown
    try {
      JobExecutionResult result = executionFuture.get();
      LOG.info("Pipeline completed successfully: {}", result);
    } catch (Exception e) {
      LOG.error("Pipeline execution failed", e);
      throw e;
    }
  }

  /** Add monitoring streams for metrics collection */
  private void addMonitoringStreams(
      DataStream<PayrollEmployee> validRecords,
      DataStream<FailedPayrollRecord> failedRecords,
      DataStream<ComplianceAuditLog> auditLogs) {
    // Metrics disabled while pipeline uses Kafka-only sinks.
  }

  /** Write validated payroll records to the configured Kafka topic. */
  private void sinkValidRecordsToKafka(DataStream<PayrollEmployee> validatedStream) {
    String topic = config.getValidPayrollTopic();

    KafkaSink<String> kafkaSink =
        KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaConfig().getBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(topic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliveryGuarantee(
                config.getKafkaConfig().isExactlyOnceEnabled()
                    ? DeliveryGuarantee.EXACTLY_ONCE
                    : DeliveryGuarantee.AT_LEAST_ONCE)
            .setTransactionalIdPrefix("payroll-valid-records-sink")
            .build();

    validatedStream
        .map(new PayrollEmployeeToJsonMap())
        .name("Serialize Valid Payroll Records")
        .returns(String.class)
        .sinkTo(kafkaSink)
        .name("Valid Payroll Kafka Sink");

    LOG.info("Configured Kafka sink for validated payroll records on topic: {}", topic);
  }

  /** Create Payroll Iceberg Sink based on environment */
  private PayrollIcebergSinkConnector createPayrollIcebergSink() {
    IcebergConfig icebergConfig = config.getIcebergConfig();

    PayrollIcebergSinkConnector.Builder builder =
        new PayrollIcebergSinkConnector.Builder()
            .warehousePath(icebergConfig.getWarehousePath())
            .catalogName(icebergConfig.getCatalogName())
            .restCatalog(icebergConfig.getRestUri())
            .restCredentials(
                icebergConfig.getRestCredentialsKey(), icebergConfig.getRestCredentialsToken());

    if (icebergConfig.getS3Endpoint() != null) {
      builder
          .withCatalogProperty("s3.endpoint", icebergConfig.getS3Endpoint())
          .withCatalogProperty(
              "s3.path-style-access", String.valueOf(icebergConfig.isPathStyleAccess()))
          .withCatalogProperty("s3.bucket-creation-enabled", "true");

      if (icebergConfig.getS3AccessKey() != null) {
        builder.withCatalogProperty("s3.access-key-id", icebergConfig.getS3AccessKey());
      }
      if (icebergConfig.getS3SecretKey() != null) {
        builder.withCatalogProperty("s3.secret-access-key", icebergConfig.getS3SecretKey());
      }
      if (icebergConfig.getS3Region() != null) {
        builder.withCatalogProperty("s3.region", icebergConfig.getS3Region());
      }
    }

    switch (config.getEnvironment()) {
      case DEVELOPMENT:
        builder.databaseName("payroll_dev").enableCompaction(false).enableEncryption(false);
        break;
      case TESTING:
        builder
            .databaseName("payroll_test")
            .tableName("test_employees_" + System.currentTimeMillis())
            .enableCompaction(false)
            .enableEncryption(false);
        break;
      case PRODUCTION:
        builder
            .databaseName("payroll")
            .enableCompaction(true)
            .compactionInterval(Duration.ofMinutes(15))
            .enableEncryption(true);
        break;
      default:
        builder.databaseName("payroll");
    }

    return builder.build();
  }

  /** Create Audit Log Iceberg Sink based on environment */
  private AuditLogIcebergSinkConnector createAuditLogIcebergSink() {
    IcebergConfig icebergConfig = config.getIcebergConfig();

    AuditLogIcebergSinkConnector.Builder builder =
        new AuditLogIcebergSinkConnector.Builder()
            .warehousePath(icebergConfig.getAuditWarehousePath())
            .catalogName(icebergConfig.getCatalogName() + "_audit")
            .restCatalog(icebergConfig.getRestUri())
            .restCredentials(
                icebergConfig.getRestCredentialsKey(), icebergConfig.getRestCredentialsToken());

    if (icebergConfig.getS3Endpoint() != null) {
      builder
          .withCatalogProperty("s3.endpoint", icebergConfig.getS3Endpoint())
          .withCatalogProperty(
              "s3.path-style-access", String.valueOf(icebergConfig.isPathStyleAccess()))
          .withCatalogProperty("s3.bucket-creation-enabled", "true");

      if (icebergConfig.getS3AccessKey() != null) {
        builder.withCatalogProperty("s3.access-key-id", icebergConfig.getS3AccessKey());
      }
      if (icebergConfig.getS3SecretKey() != null) {
        builder.withCatalogProperty("s3.secret-access-key", icebergConfig.getS3SecretKey());
      }
      if (icebergConfig.getS3Region() != null) {
        builder.withCatalogProperty("s3.region", icebergConfig.getS3Region());
      }
    }

    switch (config.getEnvironment()) {
      case DEVELOPMENT:
        builder.databaseName("compliance_dev").enableTamperDetection(false);
        break;
      case TESTING:
        builder
            .databaseName("compliance_test")
            .tableName("audit_logs_" + System.currentTimeMillis())
            .enableTamperDetection(false)
            .retentionPeriod(Duration.ofDays(30));
        break;
      case PRODUCTION:
        builder.databaseName("compliance").enableTamperDetection(true);
        break;
      default:
        builder.databaseName("compliance");
    }

    return builder.build();
  }

  /** Create Table Environment for Iceberg integration */
  private StreamTableEnvironment createTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    Configuration tableConfig = tableEnv.getConfig().getConfiguration();
    tableConfig.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
    tableConfig.setString("table.exec.sink.not-null-enforcer", "ERROR");

    return tableEnv;
  }

  private static final class PayrollEmployeeToJsonMap
      implements org.apache.flink.api.common.functions.MapFunction<PayrollEmployee, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String map(PayrollEmployee employee) throws Exception {
      return OBJECT_MAPPER.writeValueAsString(employee);
    }
  }

  /** Create Stream Execution Environment with optimized configuration */
  private StreamExecutionEnvironment createStreamExecutionEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Configure parallelism
    env.setParallelism(config.getExecutionConfig().getParallelism());

    // Configure restart strategy
    env.setRestartStrategy(
        RestartStrategies.exponentialDelayRestart(
            Time.of(1, TimeUnit.SECONDS),
            Time.of(10, TimeUnit.MINUTES),
            1.2,
            Time.of(5, TimeUnit.MINUTES),
            0.1));

    // Configure checkpointing
    if (config.getCheckpointConfig().isEnabled()) {
      env.enableCheckpointing(config.getCheckpointConfig().getInterval());

      CheckpointConfig checkpointConfig = env.getCheckpointConfig();
      checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      checkpointConfig.setMinPauseBetweenCheckpoints(config.getCheckpointConfig().getMinPause());
      checkpointConfig.setCheckpointTimeout(config.getCheckpointConfig().getTimeout());
      checkpointConfig.setMaxConcurrentCheckpoints(1);
      checkpointConfig.setExternalizedCheckpointCleanup(
          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

      // Configure state backend
      env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

      // Configure checkpoint storage
      try {
        CheckpointStorage checkpointStorage =
            new FileSystemCheckpointStorage(config.getCheckpointConfig().getStoragePath());
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);
      } catch (Exception e) {
        LOG.warn("Failed to configure checkpoint storage: {}", e.getMessage());
      }
    }

    // Configure buffer timeout for low latency
    env.setBufferTimeout(100); // 100ms

    return env;
  }

  /** Graceful shutdown of the pipeline */
  public void shutdown() {
    if (!isRunning) {
      return;
    }

    LOG.info("Shutting down payroll data quality pipeline");
    isRunning = false;

    try {
      // Cancel job execution
      if (executionFuture != null && !executionFuture.isDone()) {
        executionFuture.cancel(true);
      }

      // Shutdown infrastructure components
      if (healthCheckServer != null) {
        healthCheckServer.stop();
      }

      if (metricsCollector != null) {
        metricsCollector.stop();
      }

      if (stateManager != null) {
        stateManager.cleanup();
      }

      LOG.info("Payroll data quality pipeline shutdown completed");

    } catch (Exception e) {
      LOG.error("Error during pipeline shutdown", e);
    }
  }

  /** Get pipeline status and health information */
  public PipelineStatus getStatus() {
    return new PipelineStatus(
        isRunning,
        isHealthy,
        executionFuture != null ? executionFuture.isDone() : false,
        metricsCollector != null ? metricsCollector.getOverallMetrics() : null);
  }

  /** Pipeline status data class */
  public static class PipelineStatus {
    private final boolean running;
    private final boolean healthy;
    private final boolean completed;
    private final Object metrics;

    public PipelineStatus(boolean running, boolean healthy, boolean completed, Object metrics) {
      this.running = running;
      this.healthy = healthy;
      this.completed = completed;
      this.metrics = metrics;
    }

    public boolean isRunning() {
      return running;
    }

    public boolean isHealthy() {
      return healthy;
    }

    public boolean isCompleted() {
      return completed;
    }

    public Object getMetrics() {
      return metrics;
    }

    @Override
    public String toString() {
      return String.format(
          "PipelineStatus{running=%s, healthy=%s, completed=%s}", running, healthy, completed);
    }
  }
}
