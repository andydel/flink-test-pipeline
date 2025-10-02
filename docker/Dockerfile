# Multi-stage build for Payroll Data Quality Pipeline
FROM maven:3.9.6-eclipse-temurin-17 AS builder

# Set working directory
WORKDIR /app

# Copy Maven files first for better layer caching
COPY pom.xml .
COPY avro/ ./avro/

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src/ ./src/

# Build the application
RUN mvn clean package -DskipTests -P payroll-profile

# Runtime image
FROM flink:1.18.1-scala_2.12-java17

# Install additional tools for payroll processing
USER root
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    awscli \
    && rm -rf /var/lib/apt/lists/*

# Create payroll pipeline directories
RUN mkdir -p /opt/payroll-pipeline/lib \
             /opt/payroll-pipeline/config \
             /opt/payroll-pipeline/logs \
             /opt/payroll-pipeline/checkpoints

# Copy built JAR
COPY --from=builder /app/target/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar \
     /opt/payroll-pipeline/lib/payroll-pipeline.jar

# Copy configuration files
COPY src/main/resources/ /opt/payroll-pipeline/config/

# Set up environment for payroll pipeline
ENV PAYROLL_PIPELINE_HOME=/opt/payroll-pipeline
ENV FLINK_CONF_DIR=/opt/flink/conf
ENV JAVA_OPTS="-Djava.security.properties=/opt/payroll-pipeline/config/java.security"

# Add payroll pipeline to Flink lib directory
RUN ln -sf /opt/payroll-pipeline/lib/payroll-pipeline.jar /opt/flink/lib/

# Create non-root user for security
RUN groupadd -r payroll && useradd -r -g payroll payroll-user
RUN chown -R payroll-user:payroll /opt/payroll-pipeline
RUN chown -R payroll-user:payroll /opt/flink/log

# Switch to non-root user
USER payroll-user

# Health check for payroll pipeline
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/jobs || exit 1

# Default command
CMD ["/docker-entrypoint.sh", "help"]