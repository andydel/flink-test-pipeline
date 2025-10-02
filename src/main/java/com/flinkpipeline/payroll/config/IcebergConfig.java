package com.flinkpipeline.payroll.config;

import java.time.Duration;

/** Configuration for Iceberg data lake storage. */
public class IcebergConfig {
  private final String warehousePath;
  private final String auditWarehousePath;
  private final String catalogName;
  private final boolean compactionEnabled;
  private final Duration compactionInterval;
  private final String restUri;
  private final String restCredentialsKey;
  private final String restCredentialsToken;

  public IcebergConfig(
      String warehousePath,
      String auditWarehousePath,
      String catalogName,
      boolean compactionEnabled,
      Duration compactionInterval,
      String restUri,
      String restCredentialsKey,
      String restCredentialsToken) {
    this.warehousePath = warehousePath;
    this.auditWarehousePath = auditWarehousePath;
    this.catalogName = catalogName;
    this.compactionEnabled = compactionEnabled;
    this.compactionInterval = compactionInterval;
    this.restUri = restUri;
    this.restCredentialsKey = restCredentialsKey;
    this.restCredentialsToken = restCredentialsToken;
  }

  public String getWarehousePath() {
    return warehousePath;
  }

  public String getAuditWarehousePath() {
    return auditWarehousePath;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public boolean isCompactionEnabled() {
    return compactionEnabled;
  }

  public Duration getCompactionInterval() {
    return compactionInterval;
  }

  public String getRestUri() {
    return restUri;
  }

  public String getRestCredentialsKey() {
    return restCredentialsKey;
  }

  public String getRestCredentialsToken() {
    return restCredentialsToken;
  }
}
