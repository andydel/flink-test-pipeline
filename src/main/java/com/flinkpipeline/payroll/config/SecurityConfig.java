package com.flinkpipeline.payroll.config;

/**
 * Configuration for security and encryption settings.
 */
public class SecurityConfig {
  private final boolean piiEncryptionEnabled;
  private final boolean sslEnabled;
  private final String keystorePath;
  private final String keystorePassword;
  private final boolean authenticationEnabled;

  public SecurityConfig(boolean piiEncryptionEnabled, boolean sslEnabled, String keystorePath,
                       String keystorePassword, boolean authenticationEnabled) {
    this.piiEncryptionEnabled = piiEncryptionEnabled;
    this.sslEnabled = sslEnabled;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.authenticationEnabled = authenticationEnabled;
  }

  public boolean isPiiEncryptionEnabled() { return piiEncryptionEnabled; }
  public boolean isSslEnabled() { return sslEnabled; }
  public String getKeystorePath() { return keystorePath; }
  public String getKeystorePassword() { return keystorePassword; }
  public boolean isAuthenticationEnabled() { return authenticationEnabled; }
}