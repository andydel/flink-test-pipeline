# flink-pipeline Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-09-29

## Active Technologies
- Java 17+ (LTS) + Apache Flink 1.18+, Kafka Connector, Iceberg Connector, AWS SDK (001-build-a-flink)
- Java 17+ (LTS) + Apache Flink 1.18+, Kafka Connector, Iceberg Connector, AWS SDK, Avro Schema Registry (001-build-a-flink)
- Apache Iceberg tables on S3, Kafka topics for input/failure data (001-build-a-flink)

## Project Structure
```
src/
tests/
```

## Commands
# Add commands for Java 17+ (LTS)

## Code Style
Java 17+ (LTS): Follow standard conventions

## Recent Changes
- 001-build-a-flink: Added Java 17+ (LTS) + Apache Flink 1.18+, Kafka Connector, Iceberg Connector, AWS SDK, Avro Schema Registry
- 001-build-a-flink: Added Java 17+ (LTS) + Apache Flink 1.18+, Kafka Connector, Iceberg Connector, AWS SDK

## General notes on coding styles

All code changes must be done on a branch.  Create a branch before you start a new feature.

If you are unsure about anything, for example architectural decisions always ask.

All new classes or methods must be unit tested.  Unit tests should be written before the implementation code.

When designing new classes or functions check with the user if there is any ambiguity as to what teh function signature should look like.

When completing work run all the tests and ensure they pass.

Read and adhere to the java-coding-style-guide.md

