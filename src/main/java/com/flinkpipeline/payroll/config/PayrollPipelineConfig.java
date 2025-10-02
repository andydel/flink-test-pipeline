package com.flinkpipeline.payroll.config;

import com.flinkpipeline.payroll.models.PayrollQualityRule;
import com.flinkpipeline.payroll.operators.HRWorkflowRoutingOperator;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive configuration management for the Payroll Data Quality Pipeline. Supports multiple
 * configuration sources (properties files, environment variables, command line),
 * environment-specific settings, and validation of configuration parameters.
 *
 * <p>Configuration Hierarchy (highest to lowest priority): 1. Command line arguments 2. Environment
 * variables 3. System properties 4. External configuration files 5. Default configuration files
 * (application.properties) 6. Built-in defaults
 */
public class PayrollPipelineConfig {

  private static final Logger LOG = LoggerFactory.getLogger(PayrollPipelineConfig.class);

  // Environment types
  public enum Environment {
    DEVELOPMENT,
    TESTING,
    STAGING,
    PRODUCTION
  }

  // Core configuration
  private final Environment environment;
  private final String applicationName;
  private final String version;

  // Component configurations
  private final ExecutionConfig executionConfig;
  private final KafkaConfig kafkaConfig;
  private final ValidationConfig validationConfig;
  private final IcebergConfig icebergConfig;
  private final HrWorkflowConfig hrWorkflowConfig;
  private final SecurityConfig securityConfig;
  private final CheckpointConfig checkpointConfig;
  private final MetricsConfig metricsConfig;
  private final HealthCheckConfig healthCheckConfig;
  private final StateConfig stateConfig;

  // Constructor
  public PayrollPipelineConfig(
      Environment environment,
      String applicationName,
      String version,
      ExecutionConfig executionConfig,
      KafkaConfig kafkaConfig,
      ValidationConfig validationConfig,
      IcebergConfig icebergConfig,
      HrWorkflowConfig hrWorkflowConfig,
      SecurityConfig securityConfig,
      CheckpointConfig checkpointConfig,
      MetricsConfig metricsConfig,
      HealthCheckConfig healthCheckConfig,
      StateConfig stateConfig) {
    this.environment = environment;
    this.applicationName = applicationName;
    this.version = version;
    this.executionConfig = executionConfig;
    this.kafkaConfig = kafkaConfig;
    this.validationConfig = validationConfig;
    this.icebergConfig = icebergConfig;
    this.hrWorkflowConfig = hrWorkflowConfig;
    this.securityConfig = securityConfig;
    this.checkpointConfig = checkpointConfig;
    this.metricsConfig = metricsConfig;
    this.healthCheckConfig = healthCheckConfig;
    this.stateConfig = stateConfig;

    LOG.info(
        "Initialized PayrollPipelineConfig for environment: {}, version: {}", environment, version);
  }

  /** Create configuration from command line arguments */
  public static PayrollPipelineConfig fromArgs(String[] args) throws IOException {
    LOG.info("Loading configuration from command line arguments");

    Map<String, String> argMap = parseCommandLineArgs(args);

    // Determine environment
    Environment environment =
        Environment.valueOf(
            argMap
                .getOrDefault(
                    "environment",
                    System.getProperty(
                        "payroll.environment",
                        System.getenv().getOrDefault("PAYROLL_ENVIRONMENT", "DEVELOPMENT")))
                .toUpperCase());

    // Load configuration based on environment
    return loadConfiguration(environment, argMap);
  }

  /** Create configuration for specific environment */
  public static PayrollPipelineConfig forEnvironment(Environment environment) throws IOException {
    return loadConfiguration(environment, new HashMap<>());
  }

  /** Load configuration with precedence hierarchy */
  private static PayrollPipelineConfig loadConfiguration(
      Environment environment, Map<String, String> overrides) throws IOException {
    LOG.info("Loading configuration for environment: {}", environment);

    // Load base properties
    Properties properties = loadPropertiesWithHierarchy(environment);

    // Apply overrides
    overrides.forEach(properties::setProperty);

    // Apply environment variables
    applyEnvironmentVariables(properties);

    // Build configuration objects
    return buildConfiguration(environment, properties);
  }

  /** Load properties with configuration hierarchy */
  private static Properties loadPropertiesWithHierarchy(Environment environment)
      throws IOException {
    Properties properties = new Properties();

    // 1. Load default configuration
    loadPropertiesFromResource(properties, "application.properties");

    // 2. Load environment-specific configuration
    String envConfigFile = "application-" + environment.name().toLowerCase() + ".properties";
    loadPropertiesFromResource(properties, envConfigFile);

    // 3. Load external configuration if specified
    String externalConfigPath = System.getProperty("payroll.config.file");
    if (externalConfigPath != null) {
      loadPropertiesFromFile(properties, externalConfigPath);
    }

    return properties;
  }

  /** Load properties from classpath resource */
  private static void loadPropertiesFromResource(Properties properties, String resourceName) {
    try (InputStream input =
        PayrollPipelineConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (input != null) {
        properties.load(input);
        LOG.info("Loaded configuration from resource: {}", resourceName);
      } else {
        LOG.debug("Configuration resource not found: {}", resourceName);
      }
    } catch (IOException e) {
      LOG.warn("Failed to load configuration from resource: {}", resourceName, e);
    }
  }

  /** Load properties from external file */
  private static void loadPropertiesFromFile(Properties properties, String filePath) {
    try (InputStream input = new java.io.FileInputStream(filePath)) {
      properties.load(input);
      LOG.info("Loaded external configuration from: {}", filePath);
    } catch (IOException e) {
      LOG.warn("Failed to load external configuration from: {}", filePath, e);
    }
  }

  /** Apply environment variables to properties */
  private static void applyEnvironmentVariables(Properties properties) {
    System.getenv()
        .forEach(
            (key, value) -> {
              if (key.startsWith("PAYROLL_")) {
                String propertyKey =
                    key.toLowerCase().replace("payroll_", "payroll.").replace("_", ".");
                properties.setProperty(propertyKey, value);
                LOG.debug("Applied environment variable: {} = {}", propertyKey, value);
              }
            });
  }

  /** Parse command line arguments into key-value map */
  private static Map<String, String> parseCommandLineArgs(String[] args) {
    Map<String, String> argMap = new HashMap<>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2);
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          argMap.put(key, args[i + 1]);
          i++; // Skip next argument as it's the value
        } else {
          argMap.put(key, "true"); // Flag argument
        }
      }
    }

    LOG.debug("Parsed command line arguments: {}", argMap.keySet());
    return argMap;
  }

  /** Build configuration objects from properties */
  private static PayrollPipelineConfig buildConfiguration(
      Environment environment, Properties properties) {
    String applicationName =
        properties.getProperty("payroll.application.name", "payroll-data-quality-pipeline");
    String version = properties.getProperty("payroll.application.version", "1.0.0");

    return new PayrollPipelineConfig(
        environment,
        applicationName,
        version,
        buildExecutionConfig(properties),
        buildKafkaConfig(properties),
        buildValidationConfig(properties),
        buildIcebergConfig(properties),
        buildHrWorkflowConfig(properties),
        buildSecurityConfig(properties),
        buildCheckpointConfig(properties),
        buildMetricsConfig(properties),
        buildHealthCheckConfig(properties),
        buildStateConfig(properties));
  }

  private static ExecutionConfig buildExecutionConfig(Properties properties) {
    return new ExecutionConfig(
        Integer.parseInt(properties.getProperty("payroll.execution.parallelism", "4")),
        Integer.parseInt(properties.getProperty("payroll.execution.max.parallelism", "128")),
        Duration.ofMillis(
            Long.parseLong(properties.getProperty("payroll.execution.buffer.timeout", "100"))),
        properties.getProperty("payroll.execution.time.characteristic", "EVENT_TIME"));
  }

  private static KafkaConfig buildKafkaConfig(Properties properties) {
    return new KafkaConfig(
        properties.getProperty("payroll.kafka.bootstrap.servers", "localhost:9092"),
        properties.getProperty("payroll.kafka.schema.registry.url", "http://localhost:8081"),
        Arrays.asList(
            properties.getProperty("payroll.kafka.topics", "payroll-employees").split(",")),
        properties.getProperty("payroll.kafka.consumer.group", "flink-payroll-pipeline"),
        Boolean.parseBoolean(properties.getProperty("payroll.kafka.exactly.once.enabled", "true")),
        Duration.ofSeconds(
            Long.parseLong(properties.getProperty("payroll.kafka.consumer.timeout", "30"))));
  }

  private static ValidationConfig buildValidationConfig(Properties properties) {
    return new ValidationConfig(
        getDefaultQualityRules(),
        Boolean.parseBoolean(
            properties.getProperty("payroll.validation.duplicate.detection.enabled", "true")),
        Duration.ofMinutes(
            Long.parseLong(
                properties.getProperty("payroll.validation.duplicate.window.minutes", "60"))),
        Boolean.parseBoolean(
            properties.getProperty("payroll.validation.compliance.auditing.enabled", "true")),
        Boolean.parseBoolean(
            properties.getProperty("payroll.validation.strict.mode.enabled", "false")));
  }

  private static IcebergConfig buildIcebergConfig(Properties properties) {
    String restUri = nullIfBlank(properties.getProperty("payroll.iceberg.rest.uri"));
    String restCredentialsKey =
        nullIfBlank(properties.getProperty("payroll.iceberg.rest.credentials.key"));
    String restCredentialsToken =
        nullIfBlank(properties.getProperty("payroll.iceberg.rest.credentials.token"));

    return new IcebergConfig(
        properties.getProperty("payroll.iceberg.warehouse.path", "/tmp/iceberg/warehouse"),
        properties.getProperty("payroll.iceberg.audit.warehouse.path", "/tmp/iceberg/audit"),
        properties.getProperty("payroll.iceberg.catalog.name", "payroll_catalog"),
        Boolean.parseBoolean(properties.getProperty("payroll.iceberg.compaction.enabled", "true")),
        Duration.ofMinutes(
            Long.parseLong(
                properties.getProperty("payroll.iceberg.compaction.interval.minutes", "30"))),
        restUri,
        restCredentialsKey,
        restCredentialsToken);
  }

  private static HrWorkflowConfig buildHrWorkflowConfig(Properties properties) {
    return new HrWorkflowConfig(
        Duration.ofHours(
            Long.parseLong(properties.getProperty("payroll.hr.sla.threshold.hours", "4"))),
        Integer.parseInt(properties.getProperty("payroll.hr.max.retry.attempts", "3")),
        Boolean.parseBoolean(properties.getProperty("payroll.hr.load.balancing.enabled", "true")),
        Boolean.parseBoolean(properties.getProperty("payroll.hr.escalation.enabled", "true")),
        Duration.ofHours(
            Long.parseLong(properties.getProperty("payroll.hr.escalation.threshold.hours", "6"))),
        getDefaultHrTeamConfig());
  }

  private static SecurityConfig buildSecurityConfig(Properties properties) {
    return new SecurityConfig(
        Boolean.parseBoolean(
            properties.getProperty("payroll.security.pii.encryption.enabled", "false")),
        Boolean.parseBoolean(properties.getProperty("payroll.security.ssl.enabled", "false")),
        properties.getProperty("payroll.security.keystore.path"),
        properties.getProperty("payroll.security.keystore.password"),
        Boolean.parseBoolean(
            properties.getProperty("payroll.security.authentication.enabled", "false")));
  }

  private static CheckpointConfig buildCheckpointConfig(Properties properties) {
    return new CheckpointConfig(
        Boolean.parseBoolean(properties.getProperty("payroll.checkpoint.enabled", "true")),
        Long.parseLong(properties.getProperty("payroll.checkpoint.interval", "60000")),
        Long.parseLong(properties.getProperty("payroll.checkpoint.min.pause", "5000")),
        Long.parseLong(properties.getProperty("payroll.checkpoint.timeout", "600000")),
        properties.getProperty("payroll.checkpoint.storage.path", "/tmp/flink-checkpoints"));
  }

  private static MetricsConfig buildMetricsConfig(Properties properties) {
    return new MetricsConfig(
        Boolean.parseBoolean(properties.getProperty("payroll.metrics.enabled", "true")),
        properties.getProperty("payroll.metrics.reporter", "slf4j"),
        Duration.ofSeconds(
            Long.parseLong(properties.getProperty("payroll.metrics.interval.seconds", "60"))),
        properties.getProperty("payroll.metrics.endpoint", "http://localhost:9464/metrics"));
  }

  private static HealthCheckConfig buildHealthCheckConfig(Properties properties) {
    return new HealthCheckConfig(
        Boolean.parseBoolean(properties.getProperty("payroll.health.enabled", "true")),
        Integer.parseInt(properties.getProperty("payroll.health.port", "8080")),
        properties.getProperty("payroll.health.endpoint", "/health"),
        Duration.ofSeconds(
            Long.parseLong(properties.getProperty("payroll.health.check.interval.seconds", "30"))));
  }

  private static StateConfig buildStateConfig(Properties properties) {
    return new StateConfig(
        properties.getProperty("payroll.state.backend", "rocksdb"),
        properties.getProperty("payroll.state.storage.path", "/tmp/flink-state"),
        Boolean.parseBoolean(
            properties.getProperty("payroll.state.incremental.checkpoints", "true")),
        Long.parseLong(
            properties.getProperty("payroll.state.ttl.milliseconds", "86400000")) // 1 day
        );
  }

  private static String nullIfBlank(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  /** Get default quality rules */
  private static List<PayrollQualityRule> getDefaultQualityRules() {
    return Arrays.asList(
        PayrollQualityRule.createSSNValidationRule(),
        PayrollQualityRule.createEmailValidationRule(),
        PayrollQualityRule.createAgeRangeRule(),
        PayrollQualityRule.createWageComplianceRule());
  }

  /** Get default HR team configuration */
  private static Map<String, HRWorkflowRoutingOperator.HRTeamConfig> getDefaultHrTeamConfig() {
    Map<String, HRWorkflowRoutingOperator.HRTeamConfig> config = new HashMap<>();

    config.put(
        "CRITICAL",
        new HRWorkflowRoutingOperator.HRTeamConfig(
            Arrays.asList("hr.manager@company.com", "hr.senior.analyst@company.com"), 5));
    config.put(
        "HIGH",
        new HRWorkflowRoutingOperator.HRTeamConfig(
            Arrays.asList("hr.analyst1@company.com", "hr.analyst2@company.com"), 10));
    config.put(
        "MEDIUM",
        new HRWorkflowRoutingOperator.HRTeamConfig(
            Arrays.asList("hr.coordinator1@company.com", "hr.coordinator2@company.com"), 15));
    config.put(
        "LOW",
        new HRWorkflowRoutingOperator.HRTeamConfig(Arrays.asList("hr.assistant@company.com"), 20));

    return config;
  }

  /** Validate configuration */
  public void validate() throws IllegalArgumentException {
    LOG.info("Validating pipeline configuration");

    // Validate Kafka configuration
    if (kafkaConfig.getBootstrapServers() == null || kafkaConfig.getBootstrapServers().isEmpty()) {
      throw new IllegalArgumentException("Kafka bootstrap servers must be specified");
    }

    // Validate Iceberg configuration
    if (icebergConfig.getWarehousePath() == null || icebergConfig.getWarehousePath().isEmpty()) {
      throw new IllegalArgumentException("Iceberg warehouse path must be specified");
    }

    // Validate checkpoint configuration
    if (checkpointConfig.isEnabled()
        && (checkpointConfig.getStoragePath() == null
            || checkpointConfig.getStoragePath().isEmpty())) {
      throw new IllegalArgumentException(
          "Checkpoint storage path must be specified when checkpointing is enabled");
    }

    LOG.info("Configuration validation successful");
  }

  // Getters
  public Environment getEnvironment() {
    return environment;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getVersion() {
    return version;
  }

  public ExecutionConfig getExecutionConfig() {
    return executionConfig;
  }

  public KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  public ValidationConfig getValidationConfig() {
    return validationConfig;
  }

  public IcebergConfig getIcebergConfig() {
    return icebergConfig;
  }

  public HrWorkflowConfig getHrWorkflowConfig() {
    return hrWorkflowConfig;
  }

  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  public CheckpointConfig getCheckpointConfig() {
    return checkpointConfig;
  }

  public MetricsConfig getMetricsConfig() {
    return metricsConfig;
  }

  public HealthCheckConfig getHealthCheckConfig() {
    return healthCheckConfig;
  }

  public StateConfig getStateConfig() {
    return stateConfig;
  }

  @Override
  public String toString() {
    return String.format(
        "PayrollPipelineConfig{environment=%s, application=%s, version=%s}",
        environment, applicationName, version);
  }
}
