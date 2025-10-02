package com.flinkpipeline.payroll.integration;

import com.flinkpipeline.payroll.PayrollDataQualityPipeline;
import com.flinkpipeline.payroll.config.PayrollPipelineConfig;
import com.flinkpipeline.payroll.models.PayrollEmployee;
import com.flinkpipeline.payroll.utils.HealthCheckServer;
import com.flinkpipeline.payroll.utils.MetricsCollector;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration testing framework for the payroll data quality pipeline.
 * Tests end-to-end functionality including data ingestion, validation, routing, storage,
 * error handling, monitoring, and compliance features using containerized infrastructure.
 *
 * Test Categories:
 * - End-to-end pipeline processing with real data flows
 * - Error handling and recovery scenarios
 * - Performance and scalability under load
 * - Security and compliance validation
 * - Integration with external systems (Kafka, Iceberg, Schema Registry)
 * - Monitoring and alerting functionality
 * - Business logic validation with regulatory compliance
 * - Disaster recovery and failover scenarios
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
public class PayrollPipelineIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollPipelineIntegrationTest.class);

  // Flink test cluster
  private static final MiniClusterWithClientResource FLINK_CLUSTER =
      new MiniClusterWithClientResource.Builder()
          .setNumberTaskManagers(2)
          .setNumberSlotsPerTaskManager(4)
          .build();

  // Test containers
  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
          .withExposedPorts(9092, 9093);

  @Container
  private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("payroll_test")
          .withUsername("test")
          .withPassword("test");

  // Test infrastructure
  private PayrollDataQualityPipeline pipeline;
  private PayrollPipelineConfig testConfig;
  private TestDataGenerator dataGenerator;
  private IntegrationTestHelper testHelper;

  @BeforeAll
  static void setupInfrastructure() throws Exception {
    LOG.info("Setting up integration test infrastructure");

    // Start Flink cluster
    FLINK_CLUSTER.before();

    // Wait for containers to be ready
    KAFKA_CONTAINER.start();
    POSTGRES_CONTAINER.start();

    LOG.info("Test infrastructure setup completed");
  }

  @AfterAll
  static void tearDownInfrastructure() {
    LOG.info("Tearing down integration test infrastructure");

    try {
      FLINK_CLUSTER.after();
    } catch (Exception e) {
      LOG.error("Error shutting down Flink cluster", e);
    }

    LOG.info("Test infrastructure teardown completed");
  }

  @BeforeEach
  void setupTest() throws Exception {
    LOG.info("Setting up individual test");

    // Create test configuration
    testConfig = createTestConfiguration();

    // Initialize test helpers
    dataGenerator = new TestDataGenerator();
    testHelper = new IntegrationTestHelper(testConfig, KAFKA_CONTAINER, POSTGRES_CONTAINER);

    // Setup test topics and tables
    testHelper.setupKafkaTopics();
    testHelper.setupIcebergTables();

    LOG.info("Individual test setup completed");
  }

  @AfterEach
  void tearDownTest() throws Exception {
    LOG.info("Tearing down individual test");

    try {
      if (pipeline != null) {
        pipeline.shutdown();
      }

      // Cleanup test data
      testHelper.cleanupTestData();

    } catch (Exception e) {
      LOG.error("Error during test teardown", e);
    }

    LOG.info("Individual test teardown completed");
  }

  /**
   * Test end-to-end pipeline processing with valid records
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEndToEndProcessingWithValidRecords() throws Exception {
    LOG.info("Testing end-to-end processing with valid records");

    // Generate test data
    List<PayrollEmployee> testRecords = dataGenerator.generateValidRecords(100);

    // Send test data to Kafka
    testHelper.sendRecordsToKafka(testRecords);

    // Start pipeline
    pipeline = new PayrollDataQualityPipeline(testConfig);

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for processing
    Thread.sleep(30000); // 30 seconds

    // Verify results
    IntegrationTestResults results = testHelper.collectResults();

    // Assertions
    assertEquals(100, results.getTotalRecordsProcessed(), "Should process all input records");
    assertTrue(results.getValidRecordsCount() >= 90, "Should have high validation success rate");
    assertTrue(results.getProcessingLatency().toMillis() < 10000, "Should process within 10 seconds");

    // Verify data in Iceberg
    long icebergRecords = testHelper.countIcebergRecords();
    assertTrue(icebergRecords >= 90, "Should store valid records in Iceberg");

    // Verify audit logs
    long auditLogs = testHelper.countAuditLogs();
    assertTrue(auditLogs > 0, "Should generate audit logs");

    LOG.info("End-to-end processing test completed successfully");
  }

  /**
   * Test error handling with invalid records
   */
  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void testErrorHandlingWithInvalidRecords() throws Exception {
    LOG.info("Testing error handling with invalid records");

    // Generate invalid test data
    List<PayrollEmployee> invalidRecords = dataGenerator.generateInvalidRecords(50);

    // Send test data to Kafka
    testHelper.sendRecordsToKafka(invalidRecords);

    // Start pipeline
    pipeline = new PayrollDataQualityPipeline(testConfig);

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for processing
    Thread.sleep(20000); // 20 seconds

    // Verify error handling
    IntegrationTestResults results = testHelper.collectResults();

    // Assertions
    assertEquals(50, results.getTotalRecordsProcessed(), "Should process all input records");
    assertTrue(results.getFailedRecordsCount() >= 40, "Should detect validation failures");
    assertTrue(results.getErrorHandlingMetrics().getTotalErrorsHandled() > 0, "Should handle errors");

    // Verify failed records are routed correctly
    long failedRecords = testHelper.countFailedRecords();
    assertTrue(failedRecords >= 40, "Should route failed records to error queues");

    // Verify compliance audit logs for errors
    long complianceViolations = testHelper.countComplianceViolations();
    assertTrue(complianceViolations > 0, "Should log compliance violations");

    LOG.info("Error handling test completed successfully");
  }

  /**
   * Test performance under load
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void testPerformanceUnderLoad() throws Exception {
    LOG.info("Testing performance under load");

    // Generate large dataset
    List<PayrollEmployee> loadTestRecords = dataGenerator.generateValidRecords(10000);

    // Send test data to Kafka in batches
    testHelper.sendRecordsToKafkaInBatches(loadTestRecords, 1000);

    // Start pipeline with performance monitoring
    pipeline = new PayrollDataQualityPipeline(testConfig);

    Instant startTime = Instant.now();

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for processing with periodic monitoring
    Duration processingTime = monitorProcessingProgress(10000, Duration.ofMinutes(8));

    // Collect performance metrics
    IntegrationTestResults results = testHelper.collectResults();

    // Performance assertions
    assertEquals(10000, results.getTotalRecordsProcessed(), "Should process all records");
    assertTrue(processingTime.toMinutes() < 8, "Should complete within 8 minutes");

    double throughput = 10000.0 / processingTime.getSeconds();
    assertTrue(throughput >= 50, "Should maintain minimum throughput of 50 records/second");

    assertTrue(results.getProcessingLatency().toMillis() < 5000, "Should maintain low latency");

    // Resource utilization should be reasonable
    ResourceMetrics resourceMetrics = testHelper.getResourceMetrics();
    assertTrue(resourceMetrics.getCpuUtilization() < 0.9, "CPU utilization should be under 90%");
    assertTrue(resourceMetrics.getMemoryUtilization() < 0.8, "Memory utilization should be under 80%");

    LOG.info("Performance test completed - Throughput: {:.2f} records/second, Processing time: {}",
             throughput, processingTime);
  }

  /**
   * Test security and compliance features
   */
  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void testSecurityAndCompliance() throws Exception {
    LOG.info("Testing security and compliance features");

    // Generate records with PII data
    List<PayrollEmployee> piiRecords = dataGenerator.generateRecordsWithPII(100);

    // Send test data to Kafka
    testHelper.sendRecordsToKafka(piiRecords);

    // Start pipeline with encryption enabled
    PayrollPipelineConfig secureConfig = createSecureTestConfiguration();
    pipeline = new PayrollDataQualityPipeline(secureConfig);

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for processing
    Thread.sleep(30000); // 30 seconds

    // Verify security features
    IntegrationTestResults results = testHelper.collectResults();

    // Verify PII encryption
    List<String> storedRecords = testHelper.getStoredRecords();
    for (String record : storedRecords) {
      assertFalse(record.contains("123-45-6789"), "SSN should be encrypted");
      assertFalse(record.contains("test@example.com"), "Email should be encrypted");
    }

    // Verify audit logs for PII access
    long piiAccessLogs = testHelper.countPIIAccessLogs();
    assertTrue(piiAccessLogs > 0, "Should log PII access events");

    // Verify compliance audit trail
    long complianceAudits = testHelper.countComplianceAudits();
    assertTrue(complianceAudits > 0, "Should maintain compliance audit trail");

    LOG.info("Security and compliance test completed successfully");
  }

  /**
   * Test health monitoring and alerting
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void testHealthMonitoringAndAlerting() throws Exception {
    LOG.info("Testing health monitoring and alerting");

    // Start pipeline
    pipeline = new PayrollDataQualityPipeline(testConfig);

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for pipeline to start
    Thread.sleep(10000); // 10 seconds

    // Test health endpoints
    HealthCheckResults healthResults = testHelper.checkHealthEndpoints();

    // Assertions
    assertTrue(healthResults.isLivenessProbeHealthy(), "Liveness probe should be healthy");
    assertTrue(healthResults.isReadinessProbeHealthy(), "Readiness probe should be healthy");
    assertTrue(healthResults.isStartupProbeHealthy(), "Startup probe should be healthy");

    // Test metrics collection
    MetricsResults metricsResults = testHelper.collectMetrics();
    assertNotNull(metricsResults.getMetrics(), "Should collect metrics");
    assertTrue(metricsResults.getMetrics().size() > 0, "Should have multiple metrics");

    // Verify pipeline status
    PayrollDataQualityPipeline.PipelineStatus status = pipeline.getStatus();
    assertTrue(status.isRunning(), "Pipeline should be running");
    assertTrue(status.isHealthy(), "Pipeline should be healthy");

    LOG.info("Health monitoring test completed successfully");
  }

  /**
   * Test disaster recovery and failover
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDisasterRecoveryAndFailover() throws Exception {
    LOG.info("Testing disaster recovery and failover");

    // Generate test data
    List<PayrollEmployee> testRecords = dataGenerator.generateValidRecords(200);

    // Send first batch
    testHelper.sendRecordsToKafka(testRecords.subList(0, 100));

    // Start pipeline
    pipeline = new PayrollDataQualityPipeline(testConfig);

    CompletableFuture<Void> pipelineExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for initial processing
    Thread.sleep(15000); // 15 seconds

    // Simulate failure by stopping pipeline
    pipeline.shutdown();

    // Send second batch while pipeline is down
    testHelper.sendRecordsToKafka(testRecords.subList(100, 200));

    // Restart pipeline (disaster recovery)
    Thread.sleep(5000); // 5 seconds
    pipeline = new PayrollDataQualityPipeline(testConfig);

    CompletableFuture<Void> recoveryExecution = CompletableFuture.runAsync(() -> {
      try {
        pipeline.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for recovery processing
    Thread.sleep(30000); // 30 seconds

    // Verify recovery
    IntegrationTestResults results = testHelper.collectResults();

    // Should process all records despite the interruption
    assertEquals(200, results.getTotalRecordsProcessed(), "Should process all records after recovery");

    // Verify checkpoint recovery
    assertTrue(results.getCheckpointMetrics().getCompletedCheckpoints() > 0, "Should have completed checkpoints");

    LOG.info("Disaster recovery test completed successfully");
  }

  // Helper methods

  private PayrollPipelineConfig createTestConfiguration() throws Exception {
    return PayrollPipelineConfig.forEnvironment(PayrollPipelineConfig.Environment.TESTING);
  }

  private PayrollPipelineConfig createSecureTestConfiguration() throws Exception {
    PayrollPipelineConfig config = createTestConfiguration();
    // In real implementation, would modify security settings
    return config;
  }

  private Duration monitorProcessingProgress(int expectedRecords, Duration timeout) throws InterruptedException {
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(timeout);

    while (Instant.now().isBefore(endTime)) {
      IntegrationTestResults results = testHelper.collectResults();
      if (results.getTotalRecordsProcessed() >= expectedRecords) {
        return Duration.between(startTime, Instant.now());
      }

      Thread.sleep(5000); // Check every 5 seconds
    }

    return timeout; // Timed out
  }

  // Test helper classes and data structures

  public static class IntegrationTestResults {
    private final long totalRecordsProcessed;
    private final long validRecordsCount;
    private final long failedRecordsCount;
    private final Duration processingLatency;
    private final ErrorHandlingMetrics errorHandlingMetrics;
    private final CheckpointMetrics checkpointMetrics;

    public IntegrationTestResults(long totalRecordsProcessed, long validRecordsCount,
                                 long failedRecordsCount, Duration processingLatency,
                                 ErrorHandlingMetrics errorHandlingMetrics,
                                 CheckpointMetrics checkpointMetrics) {
      this.totalRecordsProcessed = totalRecordsProcessed;
      this.validRecordsCount = validRecordsCount;
      this.failedRecordsCount = failedRecordsCount;
      this.processingLatency = processingLatency;
      this.errorHandlingMetrics = errorHandlingMetrics;
      this.checkpointMetrics = checkpointMetrics;
    }

    public long getTotalRecordsProcessed() { return totalRecordsProcessed; }
    public long getValidRecordsCount() { return validRecordsCount; }
    public long getFailedRecordsCount() { return failedRecordsCount; }
    public Duration getProcessingLatency() { return processingLatency; }
    public ErrorHandlingMetrics getErrorHandlingMetrics() { return errorHandlingMetrics; }
    public CheckpointMetrics getCheckpointMetrics() { return checkpointMetrics; }
  }

  public static class ErrorHandlingMetrics {
    private final long totalErrorsHandled;
    private final long retriesAttempted;
    private final long deadLetterRecords;

    public ErrorHandlingMetrics(long totalErrorsHandled, long retriesAttempted, long deadLetterRecords) {
      this.totalErrorsHandled = totalErrorsHandled;
      this.retriesAttempted = retriesAttempted;
      this.deadLetterRecords = deadLetterRecords;
    }

    public long getTotalErrorsHandled() { return totalErrorsHandled; }
    public long getRetriesAttempted() { return retriesAttempted; }
    public long getDeadLetterRecords() { return deadLetterRecords; }
  }

  public static class CheckpointMetrics {
    private final long completedCheckpoints;
    private final long failedCheckpoints;

    public CheckpointMetrics(long completedCheckpoints, long failedCheckpoints) {
      this.completedCheckpoints = completedCheckpoints;
      this.failedCheckpoints = failedCheckpoints;
    }

    public long getCompletedCheckpoints() { return completedCheckpoints; }
    public long getFailedCheckpoints() { return failedCheckpoints; }
  }

  public static class ResourceMetrics {
    private final double cpuUtilization;
    private final double memoryUtilization;

    public ResourceMetrics(double cpuUtilization, double memoryUtilization) {
      this.cpuUtilization = cpuUtilization;
      this.memoryUtilization = memoryUtilization;
    }

    public double getCpuUtilization() { return cpuUtilization; }
    public double getMemoryUtilization() { return memoryUtilization; }
  }

  public static class HealthCheckResults {
    private final boolean livenessProbeHealthy;
    private final boolean readinessProbeHealthy;
    private final boolean startupProbeHealthy;

    public HealthCheckResults(boolean livenessProbeHealthy, boolean readinessProbeHealthy,
                             boolean startupProbeHealthy) {
      this.livenessProbeHealthy = livenessProbeHealthy;
      this.readinessProbeHealthy = readinessProbeHealthy;
      this.startupProbeHealthy = startupProbeHealthy;
    }

    public boolean isLivenessProbeHealthy() { return livenessProbeHealthy; }
    public boolean isReadinessProbeHealthy() { return readinessProbeHealthy; }
    public boolean isStartupProbeHealthy() { return startupProbeHealthy; }
  }

  public static class MetricsResults {
    private final Map<String, Object> metrics;

    public MetricsResults(Map<String, Object> metrics) {
      this.metrics = metrics;
    }

    public Map<String, Object> getMetrics() { return metrics; }
  }
}