package com.flinkpipeline.payroll.operators;

import com.flinkpipeline.payroll.models.ComplianceAuditLog;
import com.flinkpipeline.payroll.models.FailedPayrollRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Flink operator for intelligent routing of failed payroll records to appropriate HR
 * workflow queues. Implements priority-based routing, SLA tracking, escalation logic, and HR
 * workload balancing.
 *
 * <p>Features: - Priority-based routing (CRITICAL, HIGH, MEDIUM, LOW) - Automatic escalation for
 * SLA violations - HR team workload balancing and round-robin assignment - Retry logic for
 * transient failures - Compliance audit logging for all routing decisions - Real-time metrics and
 * dashboard integration - Integration with external HR systems (HRIS, ticketing)
 */
public class HRWorkflowRoutingOperator
    extends KeyedProcessFunction<String, FailedPayrollRecord, FailedPayrollRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HRWorkflowRoutingOperator.class);

  // Output tags for different HR workflow queues
  public static final OutputTag<FailedPayrollRecord> CRITICAL_QUEUE_TAG =
      new OutputTag<FailedPayrollRecord>("critical-queue") {};
  public static final OutputTag<FailedPayrollRecord> HIGH_PRIORITY_QUEUE_TAG =
      new OutputTag<FailedPayrollRecord>("high-priority-queue") {};
  public static final OutputTag<FailedPayrollRecord> MEDIUM_PRIORITY_QUEUE_TAG =
      new OutputTag<FailedPayrollRecord>("medium-priority-queue") {};
  public static final OutputTag<FailedPayrollRecord> LOW_PRIORITY_QUEUE_TAG =
      new OutputTag<FailedPayrollRecord>("low-priority-queue") {};
  public static final OutputTag<FailedPayrollRecord> ESCALATED_QUEUE_TAG =
      new OutputTag<FailedPayrollRecord>("escalated-queue") {};
  public static final OutputTag<ComplianceAuditLog> AUDIT_LOGS_TAG =
      new OutputTag<ComplianceAuditLog>("audit-logs") {};

  // Configuration
  private final Duration slaThreshold;
  private final int maxRetryAttempts;
  private final boolean enableLoadBalancing;
  private final boolean enableEscalation;
  private final Duration escalationThreshold;

  // HR team configuration
  private final Map<String, HRTeamConfig> hrTeamConfig;

  // State management
  private transient MapState<String, FailedRecordState> recordState;
  private transient ValueState<HRWorkloadState> workloadState;

  // Performance metrics
  private transient AtomicLong totalRecordsRouted;
  private transient AtomicLong criticalRecords;
  private transient AtomicLong escalatedRecords;
  private transient AtomicLong slaViolations;
  private transient Map<String, AtomicLong> queueMetrics;

  // SLA thresholds by priority
  private static final Map<String, Duration> SLA_THRESHOLDS =
      Map.of(
          "CRITICAL", Duration.ofMinutes(15),
          "HIGH", Duration.ofHours(2),
          "MEDIUM", Duration.ofHours(8),
          "LOW", Duration.ofHours(24));

  // Constructor
  public HRWorkflowRoutingOperator() {
    this(Duration.ofHours(4), 3, true, true, Duration.ofHours(6), getDefaultHRTeamConfig());
  }

  public HRWorkflowRoutingOperator(
      Duration slaThreshold,
      int maxRetryAttempts,
      boolean enableLoadBalancing,
      boolean enableEscalation,
      Duration escalationThreshold,
      Map<String, HRTeamConfig> hrTeamConfig) {
    this.slaThreshold = slaThreshold;
    this.maxRetryAttempts = maxRetryAttempts;
    this.enableLoadBalancing = enableLoadBalancing;
    this.enableEscalation = enableEscalation;
    this.escalationThreshold = escalationThreshold;
    this.hrTeamConfig = new HashMap<>(hrTeamConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    LOG.info(
        "Initializing HRWorkflowRoutingOperator with SLA threshold: {}, max retries: {}",
        slaThreshold,
        maxRetryAttempts);

    // Initialize state descriptors
    MapStateDescriptor<String, FailedRecordState> recordStateDescriptor =
        new MapStateDescriptor<>("failed-record-state", String.class, FailedRecordState.class);
    this.recordState = getRuntimeContext().getMapState(recordStateDescriptor);

    ValueStateDescriptor<HRWorkloadState> workloadStateDescriptor =
        new ValueStateDescriptor<>("workload-state", HRWorkloadState.class);
    this.workloadState = getRuntimeContext().getState(workloadStateDescriptor);

    // Initialize metrics
    this.totalRecordsRouted = new AtomicLong(0);
    this.criticalRecords = new AtomicLong(0);
    this.escalatedRecords = new AtomicLong(0);
    this.slaViolations = new AtomicLong(0);

    this.queueMetrics = new ConcurrentHashMap<>();
    queueMetrics.put("CRITICAL", new AtomicLong(0));
    queueMetrics.put("HIGH", new AtomicLong(0));
    queueMetrics.put("MEDIUM", new AtomicLong(0));
    queueMetrics.put("LOW", new AtomicLong(0));
    queueMetrics.put("ESCALATED", new AtomicLong(0));

    LOG.info("HRWorkflowRoutingOperator initialized successfully");
  }

  @Override
  public void processElement(
      FailedPayrollRecord record, Context context, Collector<FailedPayrollRecord> out)
      throws Exception {

    totalRecordsRouted.incrementAndGet();
    String recordKey = generateRecordKey(record);

    LOG.debug(
        "Processing failed record for routing: ID={}, Priority={}",
        record.getOriginalRecord().getEmployeeId(),
        record.getCorrectionPriority());

    try {
      // Check if this is a retry
      FailedRecordState state = recordState.get(recordKey);
      if (state == null) {
        // First time processing this record
        state = new FailedRecordState(record, Instant.now());
        recordState.put(recordKey, state);
      } else {
        // Update retry count
        state.incrementRetry();
        recordState.put(recordKey, state);
      }

      // Determine routing priority
      String routingPriority = determineRoutingPriority(record, state);

      // Check for SLA violations and escalation
      if (enableEscalation && shouldEscalate(record, state)) {
        routeToEscalatedQueue(record, state, context);
        escalatedRecords.incrementAndGet();
        return;
      }

      // Route to appropriate HR queue
      FailedPayrollRecord enrichedRecord = enrichRecordForRouting(record, state, routingPriority);
      routeToHRQueue(enrichedRecord, routingPriority, context);

      // Generate audit log
      generateRoutingAuditLog(record, routingPriority, context);

      // Schedule SLA monitoring timer
      scheduleSLATimer(recordKey, routingPriority, context);

      LOG.debug(
          "Successfully routed record to {} queue: ID={}",
          routingPriority,
          record.getOriginalRecord().getEmployeeId());

    } catch (Exception e) {
      LOG.error(
          "Failed to route payroll record: ID={}", record.getOriginalRecord().getEmployeeId(), e);

      // Route to critical queue for manual intervention
      routeToCriticalQueue(record, "ROUTING_ERROR: " + e.getMessage(), context);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<FailedPayrollRecord> out)
      throws Exception {
    String recordKey = context.getCurrentKey();
    FailedRecordState state = recordState.get(recordKey);

    if (state != null) {
      Duration timeSinceCreation =
          Duration.between(state.getCreationTime(), Instant.ofEpochMilli(timestamp));

      // Check for SLA violation
      String priority = state.getFailedRecord().getCorrectionPriority().toString();
      Duration slaLimit = SLA_THRESHOLDS.getOrDefault(priority, slaThreshold);

      if (timeSinceCreation.compareTo(slaLimit) > 0) {
        slaViolations.incrementAndGet();

        LOG.warn(
            "SLA violation detected for record: ID={}, Priority={}, Age={}",
            state.getFailedRecord().getOriginalRecord().getEmployeeId(),
            priority,
            timeSinceCreation);

        // Escalate or retry based on configuration
        if (enableEscalation) {
          escalateRecord(state, context);
        } else if (state.getRetryCount() < maxRetryAttempts) {
          retryRouting(state, context);
        }

        // Generate SLA violation audit log
        generateSLAViolationAuditLog(state.getFailedRecord(), timeSinceCreation, context);
      }
    }
  }

  /** Determine routing priority based on validation results and business rules */
  private String determineRoutingPriority(FailedPayrollRecord record, FailedRecordState state) {
    String currentPriority = record.getCorrectionPriority().toString();

    // Escalate priority based on retry count
    if (state.getRetryCount() > 0) {
      return escalatePriority(currentPriority);
    }

    // Check for regulatory compliance violations
    if (hasRegulatoryViolations(record)) {
      return "CRITICAL";
    }

    // Check for PII-related issues
    if (hasPIIViolations(record)) {
      return "HIGH";
    }

    // Check for payroll processing impact
    if (impactsPayrollProcessing(record)) {
      return "HIGH";
    }

    // Use original priority or default
    return currentPriority != null ? currentPriority : "MEDIUM";
  }

  /** Route record to appropriate HR queue based on priority */
  private void routeToHRQueue(FailedPayrollRecord record, String priority, Context context) {
    queueMetrics.get(priority).incrementAndGet();

    switch (priority) {
      case "CRITICAL":
        context.output(CRITICAL_QUEUE_TAG, record);
        criticalRecords.incrementAndGet();
        break;
      case "HIGH":
        context.output(HIGH_PRIORITY_QUEUE_TAG, record);
        break;
      case "MEDIUM":
        context.output(MEDIUM_PRIORITY_QUEUE_TAG, record);
        break;
      case "LOW":
        context.output(LOW_PRIORITY_QUEUE_TAG, record);
        break;
      default:
        LOG.warn("Unknown priority {}, routing to medium queue", priority);
        context.output(MEDIUM_PRIORITY_QUEUE_TAG, record);
    }
  }

  /** Route to escalated queue for management intervention */
  private void routeToEscalatedQueue(
      FailedPayrollRecord record, FailedRecordState state, Context context) {
    FailedPayrollRecord escalatedRecord = enrichRecordForEscalation(record, state);
    context.output(ESCALATED_QUEUE_TAG, escalatedRecord);
    queueMetrics.get("ESCALATED").incrementAndGet();

    LOG.warn(
        "Escalated record to management queue: ID={}, Retries={}",
        record.getOriginalRecord().getEmployeeId(),
        state.getRetryCount());
  }

  /** Route to critical queue for urgent manual intervention */
  private void routeToCriticalQueue(FailedPayrollRecord record, String reason, Context context) {
    FailedPayrollRecord criticalRecord =
        new FailedPayrollRecord.Builder(record)
            .correctionPriority(FailedPayrollRecord.Priority.CRITICAL)
            .hrCorrectionInstructions("URGENT: " + reason + ". Requires immediate manual review.")
            .build();

    context.output(CRITICAL_QUEUE_TAG, criticalRecord);
    criticalRecords.incrementAndGet();
    queueMetrics.get("CRITICAL").incrementAndGet();
  }

  /** Convert String priority to enum */
  private FailedPayrollRecord.Priority parsePriority(String priority) {
    try {
      return FailedPayrollRecord.Priority.valueOf(priority);
    } catch (Exception e) {
      return FailedPayrollRecord.Priority.MEDIUM;
    }
  }

  /** Enrich record with routing metadata and HR team assignment */
  private FailedPayrollRecord enrichRecordForRouting(
      FailedPayrollRecord record, FailedRecordState state, String priority) throws Exception {

    FailedPayrollRecord.Builder builder = new FailedPayrollRecord.Builder(record);

    // Update priority
    builder.correctionPriority(parsePriority(priority));

    String updatedInstructions = record.getHrCorrectionInstructions();

    if (state.getRetryCount() > 0) {
      String retryInfo = String.format("Retry #%d of %d", state.getRetryCount(), maxRetryAttempts);
      updatedInstructions = appendInstruction(updatedInstructions, retryInfo);
    }

    if (enableLoadBalancing) {
      String assignedTeamMember = assignHRTeamMember(priority);
      if (assignedTeamMember != null) {
        updatedInstructions =
            appendInstruction(updatedInstructions, "Assigned to: " + assignedTeamMember);
      }
    }

    Duration slaLimit = SLA_THRESHOLDS.getOrDefault(priority, slaThreshold);
    Instant deadline = state.getCreationTime().plus(slaLimit);
    updatedInstructions = appendInstruction(updatedInstructions, "SLA Deadline: " + deadline);

    builder.hrCorrectionInstructions(updatedInstructions);

    return builder.build();
  }

  /** Enrich record for escalation with additional context */
  private FailedPayrollRecord enrichRecordForEscalation(
      FailedPayrollRecord record, FailedRecordState state) {
    return FailedPayrollRecord.builder()
        .from(record)
        .correctionPriority(FailedPayrollRecord.Priority.CRITICAL)
        .correctionStatus(FailedPayrollRecord.CorrectionStatus.ESCALATED)
        .hrCorrectionInstructions(
            "ESCALATED: Record failed "
                + state.getRetryCount()
                + " routing attempts. "
                + "Age: "
                + Duration.between(state.getCreationTime(), Instant.now()).toString()
                + ". "
                + "Requires management review and intervention.")
        .build();
  }

  /** Assign HR team member using round-robin load balancing */
  private String assignHRTeamMember(String priority) throws Exception {
    HRWorkloadState workload = workloadState.value();
    if (workload == null) {
      workload = new HRWorkloadState();
    }

    HRTeamConfig teamConfig = hrTeamConfig.get(priority);
    if (teamConfig == null || teamConfig.getTeamMembers().isEmpty()) {
      return null;
    }

    // Simple round-robin assignment
    List<String> teamMembers = teamConfig.getTeamMembers();
    int currentIndex = workload.getAssignmentIndex(priority);
    String assignedMember = teamMembers.get(currentIndex % teamMembers.size());

    // Update assignment index
    workload.incrementAssignmentIndex(priority);
    workloadState.update(workload);

    return assignedMember;
  }

  /** Check if record should be escalated */
  private boolean shouldEscalate(FailedPayrollRecord record, FailedRecordState state) {
    // Escalate if max retries exceeded
    if (state.getRetryCount() >= maxRetryAttempts) {
      return true;
    }

    // Escalate if record age exceeds escalation threshold
    Duration age = Duration.between(state.getCreationTime(), Instant.now());
    if (age.compareTo(escalationThreshold) > 0) {
      return true;
    }

    // Escalate critical regulatory violations immediately
    if (hasRegulatoryViolations(record) && "CRITICAL".equals(record.getCorrectionPriority())) {
      return true;
    }

    return false;
  }

  /** Escalate existing record to higher priority */
  private void escalateRecord(FailedRecordState state, OnTimerContext context) throws Exception {
    FailedPayrollRecord escalatedRecord = enrichRecordForEscalation(state.getFailedRecord(), state);
    context.output(ESCALATED_QUEUE_TAG, escalatedRecord);
    escalatedRecords.incrementAndGet();

    // Clean up state
    recordState.remove(context.getCurrentKey());
  }

  /** Retry routing for records within retry limit */
  private void retryRouting(FailedRecordState state, OnTimerContext context) throws Exception {
    state.incrementRetry();
    recordState.put(context.getCurrentKey(), state);

    // Re-route with escalated priority
    String newPriority =
        escalatePriority(state.getFailedRecord().getCorrectionPriority().toString());
    FailedPayrollRecord retryRecord =
        enrichRecordForRouting(state.getFailedRecord(), state, newPriority);

    routeToHRQueue(retryRecord, newPriority, context);
    LOG.info(
        "Retrying routing for record: ID={}, Attempt={}",
        state.getFailedRecord().getOriginalRecord().getEmployeeId(),
        state.getRetryCount());
  }

  /** Generate audit logs for routing decisions */
  private void generateRoutingAuditLog(
      FailedPayrollRecord record, String priority, Context context) {
    ComplianceAuditLog auditLog =
        ComplianceAuditLog.createDataProcessingAudit(
            record.getOriginalRecord().getEmployeeId(),
            "hr_workflow_routing",
            "Record routed to " + priority + " queue",
            Map.of(
                "routing_priority", priority,
                "hr_workflow_id", record.getHrWorkflowId(),
                "correction_priority", record.getCorrectionPriority()));
    context.output(AUDIT_LOGS_TAG, auditLog);
  }

  /** Generate SLA violation audit logs */
  private void generateSLAViolationAuditLog(
      FailedPayrollRecord record, Duration age, OnTimerContext context) {
    ComplianceAuditLog violationLog =
        ComplianceAuditLog.createComplianceViolationAudit(
            record.getOriginalRecord().getEmployeeId(),
            "SLA_VIOLATION",
            "HR workflow SLA violation - record age: " + age,
            "HIGH");
    context.output(AUDIT_LOGS_TAG, violationLog);
  }

  /** Schedule SLA monitoring timer */
  private void scheduleSLATimer(String recordKey, String priority, Context context) {
    Duration slaLimit = SLA_THRESHOLDS.getOrDefault(priority, slaThreshold);
    long timerTimestamp = context.timestamp() + slaLimit.toMillis();
    context.timerService().registerProcessingTimeTimer(timerTimestamp);
  }

  /** Helper methods for business rule evaluation */
  private boolean hasRegulatoryViolations(FailedPayrollRecord record) {
    return record.getValidationErrors().stream()
        .anyMatch(
            error ->
                error.toLowerCase().contains("regulatory")
                    || error.toLowerCase().contains("compliance")
                    || error.toLowerCase().contains("ssn")
                    || error.toLowerCase().contains("minimum wage"));
  }

  private boolean hasPIIViolations(FailedPayrollRecord record) {
    return record.getValidationErrors().stream()
        .anyMatch(
            error ->
                error.toLowerCase().contains("ssn")
                    || error.toLowerCase().contains("email")
                    || error.toLowerCase().contains("pii"));
  }

  private boolean impactsPayrollProcessing(FailedPayrollRecord record) {
    return record.getValidationErrors().stream()
        .anyMatch(
            error ->
                error.toLowerCase().contains("wage")
                    || error.toLowerCase().contains("rate")
                    || error.toLowerCase().contains("payroll"));
  }

  private String escalatePriority(String currentPriority) {
    switch (currentPriority) {
      case "LOW":
        return "MEDIUM";
      case "MEDIUM":
        return "HIGH";
      case "HIGH":
        return "CRITICAL";
      case "CRITICAL":
        return "CRITICAL"; // Already at highest priority
      default:
        return "MEDIUM";
    }
  }

  private String appendInstruction(String current, String addition) {
    if (addition == null || addition.isEmpty()) {
      return current;
    }
    if (current == null || current.isEmpty()) {
      return addition;
    }
    return current + " | " + addition;
  }

  private String generateRecordKey(FailedPayrollRecord record) {
    return record.getHrWorkflowId();
  }

  /** Get default HR team configuration */
  private static Map<String, HRTeamConfig> getDefaultHRTeamConfig() {
    Map<String, HRTeamConfig> config = new HashMap<>();

    config.put(
        "CRITICAL",
        new HRTeamConfig(Arrays.asList("hr.manager@company.com", "hr.senior.analyst@company.com")));
    config.put(
        "HIGH",
        new HRTeamConfig(
            Arrays.asList(
                "hr.analyst1@company.com",
                "hr.analyst2@company.com",
                "hr.specialist@company.com")));
    config.put(
        "MEDIUM",
        new HRTeamConfig(
            Arrays.asList("hr.coordinator1@company.com", "hr.coordinator2@company.com")));
    config.put(
        "LOW",
        new HRTeamConfig(Arrays.asList("hr.intern@company.com", "hr.assistant@company.com")));

    return config;
  }

  /** Get routing statistics */
  public RoutingStatistics getStatistics() {
    Map<String, Long> queueCounts = new HashMap<>();
    queueMetrics.forEach((key, value) -> queueCounts.put(key, value.get()));

    return new RoutingStatistics(
        totalRecordsRouted.get(),
        criticalRecords.get(),
        escalatedRecords.get(),
        slaViolations.get(),
        queueCounts);
  }

  /** Data classes for state management and configuration */
  public static class FailedRecordState implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final FailedPayrollRecord failedRecord;
    private final Instant creationTime;
    private int retryCount;

    public FailedRecordState(FailedPayrollRecord failedRecord, Instant creationTime) {
      this.failedRecord = failedRecord;
      this.creationTime = creationTime;
      this.retryCount = 0;
    }

    public FailedPayrollRecord getFailedRecord() {
      return failedRecord;
    }

    public Instant getCreationTime() {
      return creationTime;
    }

    public int getRetryCount() {
      return retryCount;
    }

    public void incrementRetry() {
      this.retryCount++;
    }
  }

  public static class HRWorkloadState implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<String, Integer> assignmentIndices = new HashMap<>();

    public int getAssignmentIndex(String priority) {
      return assignmentIndices.getOrDefault(priority, 0);
    }

    public void incrementAssignmentIndex(String priority) {
      assignmentIndices.put(priority, getAssignmentIndex(priority) + 1);
    }
  }

  public static class HRTeamConfig implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final List<String> teamMembers;
    private final int maxConcurrentCases;

    public HRTeamConfig(List<String> teamMembers) {
      this(teamMembers, 10);
    }

    public HRTeamConfig(List<String> teamMembers, int maxConcurrentCases) {
      this.teamMembers = teamMembers;
      this.maxConcurrentCases = maxConcurrentCases;
    }

    public List<String> getTeamMembers() {
      return teamMembers;
    }

    public int getMaxConcurrentCases() {
      return maxConcurrentCases;
    }
  }

  public static class RoutingStatistics implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final long totalRouted;
    private final long criticalRecords;
    private final long escalatedRecords;
    private final long slaViolations;
    private final Map<String, Long> queueCounts;

    public RoutingStatistics(
        long totalRouted,
        long criticalRecords,
        long escalatedRecords,
        long slaViolations,
        Map<String, Long> queueCounts) {
      this.totalRouted = totalRouted;
      this.criticalRecords = criticalRecords;
      this.escalatedRecords = escalatedRecords;
      this.slaViolations = slaViolations;
      this.queueCounts = new HashMap<>(queueCounts);
    }

    public long getTotalRouted() {
      return totalRouted;
    }

    public long getCriticalRecords() {
      return criticalRecords;
    }

    public long getEscalatedRecords() {
      return escalatedRecords;
    }

    public long getSlaViolations() {
      return slaViolations;
    }

    public Map<String, Long> getQueueCounts() {
      return queueCounts;
    }

    @Override
    public String toString() {
      return String.format(
          "RoutingStatistics{total=%d, critical=%d, escalated=%d, slaViolations=%d, queues=%s}",
          totalRouted, criticalRecords, escalatedRecords, slaViolations, queueCounts);
    }
  }
}
