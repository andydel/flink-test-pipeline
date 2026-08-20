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
  private final String s3Endpoint;
  private final boolean pathStyleAccess;
  private final String s3AccessKey;
  private final String s3SecretKey;
  private final String s3Region;

  public IcebergConfig(
      String warehousePath,
      String auditWarehousePath,
      String catalogName,
      boolean compactionEnabled,
      Duration compactionInterval,
      String restUri,
      String restCredentialsKey,
      String restCredentialsToken,
      String s3Endpoint,
      boolean pathStyleAccess,
      String s3AccessKey,
      String s3SecretKey,
      String s3Region) {
    this.warehousePath = warehousePath;
    this.auditWarehousePath = auditWarehousePath;
    this.catalogName = catalogName;
    this.compactionEnabled = compactionEnabled;
    this.compactionInterval = compactionInterval;
    this.restUri = restUri;
    this.restCredentialsKey = restCredentialsKey;
    this.restCredentialsToken = restCredentialsToken;
    this.s3Endpoint = s3Endpoint;
    this.pathStyleAccess = pathStyleAccess;
    this.s3AccessKey = s3AccessKey;
    this.s3SecretKey = s3SecretKey;
    this.s3Region = s3Region;
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

  public String getS3Endpoint() {
    return s3Endpoint;
  }

  public boolean isPathStyleAccess() {
    return pathStyleAccess;
  }

  public String getS3AccessKey() {
    return s3AccessKey;
  }

  public String getS3SecretKey() {
    return s3SecretKey;
  }

  public String getS3Region() {
    return s3Region;
  }
}
