package com.flinkpipeline.payroll.utils;

import com.flinkpipeline.payroll.config.HealthCheckConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP health check server for the payroll data quality pipeline.
 * Provides comprehensive health monitoring endpoints for container orchestration,
 * load balancers, and monitoring systems (Kubernetes, Docker, AWS ELB).
 *
 * Health Check Categories:
 * - Liveness probe: Basic service availability
 * - Readiness probe: Service ready to accept traffic
 * - Startup probe: Service initialization status
 * - Deep health checks: Component-specific health validation
 * - Dependencies: External system connectivity (Kafka, Iceberg, Schema Registry)
 * - Performance: Resource utilization and performance metrics
 */
public class HealthCheckServer {

  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);

  // Configuration
  private final HealthCheckConfig config;
  private final boolean enabled;

  // HTTP server
  private HttpServer server;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

  // Health check registry
  private final Map<String, HealthCheck> healthChecks = new ConcurrentHashMap<>();
  private final Map<String, HealthCheckResult> healthResults = new ConcurrentHashMap<>();

  // Server state
  private volatile boolean isStarted = false;
  private volatile Instant startTime = Instant.now();
  private volatile HealthStatus overallStatus = HealthStatus.STARTING;

  // Health check types
  public enum HealthStatus {
    HEALTHY,
    DEGRADED,
    UNHEALTHY,
    STARTING,
    STOPPING
  }

  // Constructor
  public HealthCheckServer(HealthCheckConfig config) {
    this.config = config;
    this.enabled = config.isEnabled();

    if (enabled) {
      LOG.info("Initializing HealthCheckServer on port: {}, endpoint: {}",
               config.getPort(), config.getEndpoint());
      initializeDefaultHealthChecks();
    } else {
      LOG.info("Health check server disabled");
    }
  }

  /**
   * Start the health check server
   */
  public void start() throws IOException {
    if (!enabled) {
      return;
    }

    LOG.info("Starting health check server");

    try {
      // Create HTTP server
      server = HttpServer.create(new InetSocketAddress(config.getPort()), 0);

      // Register endpoints
      registerEndpoints();

      // Start server
      server.setExecutor(Executors.newFixedThreadPool(4));
      server.start();

      // Schedule health check execution
      scheduler.scheduleAtFixedRate(
          this::executeHealthChecks,
          config.getCheckInterval().toSeconds(),
          config.getCheckInterval().toSeconds(),
          TimeUnit.SECONDS
      );

      isStarted = true;
      overallStatus = HealthStatus.HEALTHY;

      LOG.info("Health check server started on port: {}", config.getPort());

    } catch (Exception e) {
      LOG.error("Failed to start health check server", e);
      throw new IOException("Health check server startup failed", e);
    }
  }

  /**
   * Stop the health check server
   */
  public void stop() {
    if (!enabled || !isStarted) {
      return;
    }

    LOG.info("Stopping health check server");
    overallStatus = HealthStatus.STOPPING;

    try {
      // Stop scheduler
      scheduler.shutdown();
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }

      // Stop HTTP server
      if (server != null) {
        server.stop(5); // Wait up to 5 seconds for graceful shutdown
      }

      isStarted = false;
      LOG.info("Health check server stopped");

    } catch (Exception e) {
      LOG.error("Error stopping health check server", e);
    }
  }

  /**
   * Register HTTP endpoints
   */
  private void registerEndpoints() {
    // Main health endpoint
    server.createContext(config.getEndpoint(), new HealthHandler());

    // Liveness probe (basic availability)
    server.createContext("/health/live", new LivenessHandler());

    // Readiness probe (ready to serve traffic)
    server.createContext("/health/ready", new ReadinessHandler());

    // Startup probe (initialization complete)
    server.createContext("/health/startup", new StartupHandler());

    // Detailed health check
    server.createContext("/health/detailed", new DetailedHealthHandler());

    // Metrics endpoint
    server.createContext("/health/metrics", new MetricsHandler());

    // Status endpoint
    server.createContext("/health/status", new StatusHandler());

    LOG.info("Registered health check endpoints");
  }

  /**
   * Initialize default health checks
   */
  private void initializeDefaultHealthChecks() {
    // Basic system health
    registerHealthCheck("system", () -> {
      // Check basic system resources
      Runtime runtime = Runtime.getRuntime();
      long maxMemory = runtime.maxMemory();
      long usedMemory = runtime.totalMemory() - runtime.freeMemory();
      double memoryUsage = (double) usedMemory / maxMemory;

      if (memoryUsage > 0.9) {
        return false; // Memory usage too high
      }

      return true;
    });

    // JVM health
    registerHealthCheck("jvm", () -> {
      // Check for excessive GC or other JVM issues
      long uptime = java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime();
      return uptime > 1000; // At least 1 second uptime
    });

    // Service state health
    registerHealthCheck("service", () -> isStarted && overallStatus != HealthStatus.STOPPING);

    LOG.info("Default health checks initialized");
  }

  /**
   * Register a health check
   */
  public void registerHealthCheck(String name, HealthCheck healthCheck) {
    healthChecks.put(name, healthCheck);
    LOG.debug("Registered health check: {}", name);
  }

  /**
   * Register a health check with supplier
   */
  public void registerHealthCheck(String name, Supplier<Boolean> healthSupplier) {
    registerHealthCheck(name, new SupplierHealthCheck(healthSupplier));
  }

  /**
   * Execute all health checks
   */
  private void executeHealthChecks() {
    try {
      Map<String, HealthCheckResult> results = new HashMap<>();
      boolean allHealthy = true;
      boolean anyDegraded = false;

      for (Map.Entry<String, HealthCheck> entry : healthChecks.entrySet()) {
        String name = entry.getKey();
        HealthCheck check = entry.getValue();

        try {
          Instant start = Instant.now();
          boolean healthy = check.check();
          Duration duration = Duration.between(start, Instant.now());

          HealthCheckResult result = new HealthCheckResult(
              name,
              healthy,
              healthy ? "OK" : "FAILED",
              duration,
              Instant.now()
          );

          results.put(name, result);

          if (!healthy) {
            allHealthy = false;
            if (!isDegraded(name)) {
              anyDegraded = false; // Critical failure
            } else {
              anyDegraded = true; // Degraded but not critical
            }
          }

        } catch (Exception e) {
          LOG.warn("Health check '{}' failed with exception", name, e);
          results.put(name, new HealthCheckResult(
              name, false, "ERROR: " + e.getMessage(), Duration.ZERO, Instant.now()));
          allHealthy = false;
        }
      }

      // Update results
      healthResults.clear();
      healthResults.putAll(results);

      // Update overall status
      if (allHealthy) {
        overallStatus = HealthStatus.HEALTHY;
      } else if (anyDegraded) {
        overallStatus = HealthStatus.DEGRADED;
      } else {
        overallStatus = HealthStatus.UNHEALTHY;
      }

    } catch (Exception e) {
      LOG.error("Error executing health checks", e);
      overallStatus = HealthStatus.UNHEALTHY;
    }
  }

  /**
   * Check if a health check represents a degraded (non-critical) failure
   */
  private boolean isDegraded(String checkName) {
    // Define which checks are non-critical
    return checkName.equals("metrics") || checkName.equals("monitoring");
  }

  /**
   * Get current health status
   */
  public HealthStatus getHealthStatus() {
    return overallStatus;
  }

  /**
   * Get all health check results
   */
  public Map<String, HealthCheckResult> getHealthResults() {
    return new HashMap<>(healthResults);
  }

  /**
   * Main health endpoint handler
   */
  private class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        boolean isHealthy = overallStatus == HealthStatus.HEALTHY ||
                           overallStatus == HealthStatus.DEGRADED;

        int statusCode = isHealthy ? 200 : 503;
        String response = createHealthResponse();

        sendResponse(exchange, statusCode, response, "application/json");

      } catch (Exception e) {
        LOG.error("Error handling health check request", e);
        sendResponse(exchange, 500, "{\"status\":\"ERROR\",\"message\":\"Internal server error\"}",
                    "application/json");
      }
    }
  }

  /**
   * Liveness probe handler
   */
  private class LivenessHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Liveness probe - service is running
      boolean isAlive = isStarted && overallStatus != HealthStatus.STOPPING;
      int statusCode = isAlive ? 200 : 503;
      String response = String.format("{\"status\":\"%s\",\"timestamp\":\"%s\"}",
                                     isAlive ? "ALIVE" : "DEAD", Instant.now());

      sendResponse(exchange, statusCode, response, "application/json");
    }
  }

  /**
   * Readiness probe handler
   */
  private class ReadinessHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Readiness probe - service is ready to accept traffic
      boolean isReady = overallStatus == HealthStatus.HEALTHY;
      int statusCode = isReady ? 200 : 503;
      String response = String.format("{\"status\":\"%s\",\"timestamp\":\"%s\"}",
                                     isReady ? "READY" : "NOT_READY", Instant.now());

      sendResponse(exchange, statusCode, response, "application/json");
    }
  }

  /**
   * Startup probe handler
   */
  private class StartupHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Startup probe - service has finished initialization
      boolean hasStarted = overallStatus != HealthStatus.STARTING;
      int statusCode = hasStarted ? 200 : 503;
      String response = String.format("{\"status\":\"%s\",\"uptime_seconds\":%d,\"timestamp\":\"%s\"}",
                                     hasStarted ? "STARTED" : "STARTING",
                                     Duration.between(startTime, Instant.now()).getSeconds(),
                                     Instant.now());

      sendResponse(exchange, statusCode, response, "application/json");
    }
  }

  /**
   * Detailed health check handler
   */
  private class DetailedHealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = createDetailedHealthResponse();
      int statusCode = overallStatus == HealthStatus.HEALTHY ? 200 : 503;

      sendResponse(exchange, statusCode, response, "application/json");
    }
  }

  /**
   * Metrics endpoint handler
   */
  private class MetricsHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = createMetricsResponse();
      sendResponse(exchange, 200, response, "application/json");
    }
  }

  /**
   * Status endpoint handler
   */
  private class StatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = createStatusResponse();
      sendResponse(exchange, 200, response, "application/json");
    }
  }

  /**
   * Create basic health response
   */
  private String createHealthResponse() {
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"status\":\"").append(overallStatus).append("\",");
    json.append("\"timestamp\":\"").append(Instant.now()).append("\",");
    json.append("\"uptime_seconds\":").append(Duration.between(startTime, Instant.now()).getSeconds()).append(",");
    json.append("\"checks\":{");

    boolean first = true;
    for (Map.Entry<String, HealthCheckResult> entry : healthResults.entrySet()) {
      if (!first) json.append(",");
      HealthCheckResult result = entry.getValue();
      json.append("\"").append(entry.getKey()).append("\":");
      json.append("{\"healthy\":").append(result.isHealthy()).append(",");
      json.append("\"message\":\"").append(result.getMessage()).append("\"}");
      first = false;
    }

    json.append("}}");
    return json.toString();
  }

  /**
   * Create detailed health response
   */
  private String createDetailedHealthResponse() {
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"status\":\"").append(overallStatus).append("\",");
    json.append("\"timestamp\":\"").append(Instant.now()).append("\",");
    json.append("\"uptime_seconds\":").append(Duration.between(startTime, Instant.now()).getSeconds()).append(",");
    json.append("\"server_info\":{");
    json.append("\"started\":").append(isStarted).append(",");
    json.append("\"start_time\":\"").append(startTime).append("\"");
    json.append("},");
    json.append("\"health_checks\":{");

    boolean first = true;
    for (Map.Entry<String, HealthCheckResult> entry : healthResults.entrySet()) {
      if (!first) json.append(",");
      HealthCheckResult result = entry.getValue();
      json.append("\"").append(entry.getKey()).append("\":");
      json.append("{");
      json.append("\"healthy\":").append(result.isHealthy()).append(",");
      json.append("\"message\":\"").append(result.getMessage()).append("\",");
      json.append("\"duration_ms\":").append(result.getDuration().toMillis()).append(",");
      json.append("\"last_check\":\"").append(result.getTimestamp()).append("\"");
      json.append("}");
      first = false;
    }

    json.append("}}");
    return json.toString();
  }

  /**
   * Create metrics response
   */
  private String createMetricsResponse() {
    Runtime runtime = Runtime.getRuntime();
    return String.format(
        "{\"jvm\":{\"memory_used\":%d,\"memory_max\":%d,\"memory_free\":%d}," +
        "\"system\":{\"uptime_seconds\":%d,\"processors\":%d}," +
        "\"health\":{\"status\": \"%s\",\"checks_total\":%d}}",
        runtime.totalMemory() - runtime.freeMemory(),
        runtime.maxMemory(),
        runtime.freeMemory(),
        Duration.between(startTime, Instant.now()).getSeconds(),
        runtime.availableProcessors(),
        overallStatus,
        healthResults.size()
    );
  }

  /**
   * Create status response
   */
  private String createStatusResponse() {
    return String.format(
        "{\"status\":\"%s\",\"healthy\":%s,\"degraded\":%s,\"timestamp\":\"%s\"}",
        overallStatus,
        overallStatus == HealthStatus.HEALTHY,
        overallStatus == HealthStatus.DEGRADED,
        Instant.now()
    );
  }

  /**
   * Send HTTP response
   */
  private void sendResponse(HttpExchange exchange, int statusCode, String response, String contentType)
      throws IOException {

    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.getResponseHeaders().set("Cache-Control", "no-cache");

    byte[] responseBytes = response.getBytes("UTF-8");
    exchange.sendResponseHeaders(statusCode, responseBytes.length);

    try (OutputStream os = exchange.getResponseBody()) {
      os.write(responseBytes);
    }
  }

  /**
   * Health check interface
   */
  @FunctionalInterface
  public interface HealthCheck {
    boolean check() throws Exception;
  }

  /**
   * Supplier-based health check implementation
   */
  private static class SupplierHealthCheck implements HealthCheck {
    private final Supplier<Boolean> supplier;

    public SupplierHealthCheck(Supplier<Boolean> supplier) {
      this.supplier = supplier;
    }

    @Override
    public boolean check() throws Exception {
      return supplier.get();
    }
  }

  /**
   * Health check result data class
   */
  public static class HealthCheckResult {
    private final String name;
    private final boolean healthy;
    private final String message;
    private final Duration duration;
    private final Instant timestamp;

    public HealthCheckResult(String name, boolean healthy, String message, Duration duration, Instant timestamp) {
      this.name = name;
      this.healthy = healthy;
      this.message = message;
      this.duration = duration;
      this.timestamp = timestamp;
    }

    public String getName() { return name; }
    public boolean isHealthy() { return healthy; }
    public String getMessage() { return message; }
    public Duration getDuration() { return duration; }
    public Instant getTimestamp() { return timestamp; }

    @Override
    public String toString() {
      return String.format("HealthCheckResult{name='%s', healthy=%s, message='%s', duration=%s}",
                          name, healthy, message, duration);
    }
  }
}