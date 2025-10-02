# Payroll Data Generator for Testing
FROM openjdk:17-jdk-slim

# Install required tools
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the built JAR (assumes it's built separately)
COPY target/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar app.jar

# Copy test data and scripts
COPY scripts/test-data/ ./test-data/
COPY scripts/generate-payroll-data.sh ./

# Set permissions
RUN chmod +x generate-payroll-data.sh

# Environment variables for data generation
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV PAYROLL_TOPIC=payroll-employees
ENV GENERATION_RATE=100
ENV GENERATION_MODE=continuous

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://schema-registry:8081/subjects || exit 1

# Default command
CMD ["./generate-payroll-data.sh"]