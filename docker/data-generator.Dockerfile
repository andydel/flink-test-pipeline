# Payroll Data Generator for Testing
FROM eclipse-temurin:17-jdk-jammy

# Install required tools
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir kafka-python fastavro requests

# Create app directory
WORKDIR /app

# Copy the built JAR (assumes it's built separately)
COPY target/payroll-data-quality-pipeline-1.0.0-SNAPSHOT.jar app.jar

# Copy test data and generator scripts
RUN mkdir -p scripts
COPY docker/data-generator/test-data/ ./test-data/
COPY docker/data-generator/generate-payroll-data.py ./scripts/generate-payroll-data.py
COPY docker/data-generator/generate-payroll-data.sh ./

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
