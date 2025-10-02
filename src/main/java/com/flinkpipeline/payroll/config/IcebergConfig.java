package com.flinkpipeline.payroll.config;

import java.time.Duration;

/**
 * Configuration for Iceberg data lake storage.
 */
public class IcebergConfig {
  private final String warehousePath;
  private final String auditWarehousePath;
  private final String catalogName;
  private final boolean compactionEnabled;
  private final Duration compactionInterval;

  public IcebergConfig(String warehousePath, String auditWarehousePath, String catalogName,
                      boolean compactionEnabled, Duration compactionInterval) {
    this.warehousePath = warehousePath;
    this.auditWarehousePath = auditWarehousePath;
    this.catalogName = catalogName;
    this.compactionEnabled = compactionEnabled;
    this.compactionInterval = compactionInterval;
  }

  public String getWarehousePath() { return warehousePath; }
  public String getAuditWarehousePath() { return auditWarehousePath; }
  public String getCatalogName() { return catalogName; }
  public boolean isCompactionEnabled() { return compactionEnabled; }
  public Duration getCompactionInterval() { return compactionInterval; }
}