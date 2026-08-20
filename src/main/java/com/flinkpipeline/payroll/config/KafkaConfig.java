package com.flinkpipeline.payroll.config;

import java.time.Duration;
import java.util.List;

/** Configuration for Kafka source connector. */
public class KafkaConfig {
  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final List<String> topics;
  private final String consumerGroup;
  private final boolean exactlyOnceEnabled;
  private final Duration consumerTimeout;

  public KafkaConfig(
      String bootstrapServers,
      String schemaRegistryUrl,
      List<String> topics,
      String consumerGroup,
      boolean exactlyOnceEnabled,
      Duration consumerTimeout) {
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.topics = topics;
    this.consumerGroup = consumerGroup;
    this.exactlyOnceEnabled = exactlyOnceEnabled;
    this.consumerTimeout = consumerTimeout;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public List<String> getTopics() {
    return topics;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public boolean isExactlyOnceEnabled() {
    return exactlyOnceEnabled;
  }

  public Duration getConsumerTimeout() {
    return consumerTimeout;
  }
}
