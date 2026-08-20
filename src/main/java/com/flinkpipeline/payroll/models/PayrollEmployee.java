package com.flinkpipeline.payroll.models;

import java.util.Objects;

/**
 * Represents an employee payroll record from the Avro schema. Contains all fields required for
 * payroll processing and validation.
 *
 * <p>This class follows the data model specification and includes validation constraints.
 */
public class PayrollEmployee {

  private Integer employeeId;
  private String firstName;
  private String lastName;
  private Integer age;
  private String ssn;
  private Integer hourlyRate; // Stored in cents for precision
  private String gender;
  private String email;
  private String sourceSystem;
  private Long ingestionTimestamp;
  private String pipelineVersion;

  // Default constructor for serialization
  public PayrollEmployee() {}

  // Constructor with all fields
  public PayrollEmployee(
      Integer employeeId,
      String firstName,
      String lastName,
      Integer age,
      String ssn,
      Integer hourlyRate,
      String gender,
      String email) {
    this.employeeId = employeeId;
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
    this.ssn = ssn;
    this.hourlyRate = hourlyRate;
    this.gender = gender;
    this.email = email;
  }

  // Getters and Setters

  public Integer getEmployeeId() {
    return employeeId;
  }

  public void setEmployeeId(Integer employeeId) {
    this.employeeId = employeeId;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public Integer getAge() {
    return age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public String getSsn() {
    return ssn;
  }

  public void setSsn(String ssn) {
    this.ssn = ssn;
  }

  public Integer getHourlyRate() {
    return hourlyRate;
  }

  public void setHourlyRate(Integer hourlyRate) {
    this.hourlyRate = hourlyRate;
  }

  public String getGender() {
    return gender;
  }

  public void setGender(String gender) {
    this.gender = gender;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getSourceSystem() {
    return sourceSystem;
  }

  public void setSourceSystem(String sourceSystem) {
    this.sourceSystem = sourceSystem;
  }

  public Long getIngestionTimestamp() {
    return ingestionTimestamp;
  }

  public void setIngestionTimestamp(Long ingestionTimestamp) {
    this.ingestionTimestamp = ingestionTimestamp;
  }

  public String getPipelineVersion() {
    return pipelineVersion;
  }

  public void setPipelineVersion(String pipelineVersion) {
    this.pipelineVersion = pipelineVersion;
  }

  // Utility methods

  /** Converts hourly rate from cents to dollars for display */
  public double getHourlyRateInDollars() {
    return hourlyRate != null ? hourlyRate / 100.0 : 0.0;
  }

  /** Sets hourly rate from dollars (converts to cents) */
  public void setHourlyRateFromDollars(double dollars) {
    this.hourlyRate = (int) Math.round(dollars * 100);
  }

  /** Checks if this employee record contains PII fields */
  public boolean containsPII() {
    return ssn != null || email != null;
  }

  /** Creates a copy of this employee with PII fields masked for logging */
  public PayrollEmployee createMaskedCopy() {
    PayrollEmployee masked = new PayrollEmployee();
    masked.employeeId = this.employeeId;
    masked.firstName = this.firstName;
    masked.lastName = this.lastName;
    masked.age = this.age;
    masked.ssn = this.ssn != null ? "***-**-****" : null;
    masked.hourlyRate = this.hourlyRate;
    masked.gender = this.gender;
    masked.email =
        this.email != null ? "*****@" + this.email.substring(this.email.indexOf('@') + 1) : null;
    return masked;
  }

  /** Validates that all required fields are present */
  public boolean hasRequiredFields() {
    return employeeId != null
        && firstName != null
        && !firstName.trim().isEmpty()
        && lastName != null
        && !lastName.trim().isEmpty()
        && age != null
        && ssn != null
        && !ssn.trim().isEmpty()
        && hourlyRate != null
        && gender != null
        && !gender.trim().isEmpty()
        && email != null
        && !email.trim().isEmpty();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    PayrollEmployee that = (PayrollEmployee) obj;
    return Objects.equals(employeeId, that.employeeId)
        && Objects.equals(firstName, that.firstName)
        && Objects.equals(lastName, that.lastName)
        && Objects.equals(age, that.age)
        && Objects.equals(ssn, that.ssn)
        && Objects.equals(hourlyRate, that.hourlyRate)
        && Objects.equals(gender, that.gender)
        && Objects.equals(email, that.email);
  }

  @Override
  public int hashCode() {
    return Objects.hash(employeeId, firstName, lastName, age, ssn, hourlyRate, gender, email);
  }

  @Override
  public String toString() {
    // Return masked version to avoid accidentally logging PII
    return createMaskedCopy().toStringWithPII();
  }

  /** Returns string representation with PII fields visible (use with caution) */
  public String toStringWithPII() {
    return "PayrollEmployee{"
        + "employeeId="
        + employeeId
        + ", firstName='"
        + firstName
        + '\''
        + ", lastName='"
        + lastName
        + '\''
        + ", age="
        + age
        + ", ssn='"
        + ssn
        + '\''
        + ", hourlyRate="
        + hourlyRate
        + ", gender='"
        + gender
        + '\''
        + ", email='"
        + email
        + '\''
        + '}';
  }

  /** Builder pattern for creating PayrollEmployee instances */
  public static class Builder {
    private Integer employeeId;
    private String firstName;
    private String lastName;
    private Integer age;
    private String ssn;
    private Integer hourlyRate;
    private String gender;
    private String email;
    private String sourceSystem;
    private Long ingestionTimestamp;
    private String pipelineVersion;

    public Builder from(PayrollEmployee employee) {
      if (employee == null) {
        return this;
      }
      this.employeeId = employee.getEmployeeId();
      this.firstName = employee.getFirstName();
      this.lastName = employee.getLastName();
      this.age = employee.getAge();
      this.ssn = employee.getSsn();
      this.hourlyRate = employee.getHourlyRate();
      this.gender = employee.getGender();
      this.email = employee.getEmail();
      this.sourceSystem = employee.getSourceSystem();
      this.ingestionTimestamp = employee.getIngestionTimestamp();
      this.pipelineVersion = employee.getPipelineVersion();
      return this;
    }

    public Builder employeeId(Integer employeeId) {
      this.employeeId = employeeId;
      return this;
    }

    public Builder firstName(String firstName) {
      this.firstName = firstName;
      return this;
    }

    public Builder lastName(String lastName) {
      this.lastName = lastName;
      return this;
    }

    public Builder age(Integer age) {
      this.age = age;
      return this;
    }

    public Builder ssn(String ssn) {
      this.ssn = ssn;
      return this;
    }

    public Builder hourlyRate(Integer hourlyRate) {
      this.hourlyRate = hourlyRate;
      return this;
    }

    public Builder hourlyRateFromDollars(double dollars) {
      this.hourlyRate = (int) Math.round(dollars * 100);
      return this;
    }

    public Builder gender(String gender) {
      this.gender = gender;
      return this;
    }

    public Builder email(String email) {
      this.email = email;
      return this;
    }

    public Builder sourceSystem(String sourceSystem) {
      this.sourceSystem = sourceSystem;
      return this;
    }

    public Builder ingestionTimestamp(Long ingestionTimestamp) {
      this.ingestionTimestamp = ingestionTimestamp;
      return this;
    }

    public Builder pipelineVersion(String pipelineVersion) {
      this.pipelineVersion = pipelineVersion;
      return this;
    }

    public PayrollEmployee build() {
      PayrollEmployee employee =
          new PayrollEmployee(employeeId, firstName, lastName, age, ssn, hourlyRate, gender, email);
      employee.setSourceSystem(sourceSystem);
      employee.setIngestionTimestamp(ingestionTimestamp);
      employee.setPipelineVersion(pipelineVersion);
      return employee;
    }
  }

  /** Creates a new builder instance */
  public static Builder builder() {
    return new Builder();
  }
}
