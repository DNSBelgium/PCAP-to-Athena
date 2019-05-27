package be.dnsbelgium.data.pcap.utils;

import com.maxmind.geoip2.model.AsnResponse;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

public class AutonomousSystem {

  private final Integer autonomousSystemNumber;
  private final String autonomousSystemOrganization;
  private final String ipAddress;

  public AutonomousSystem(Integer autonomousSystemNumber, String autonomousSystemOrganization, String ipAddress) {
    this.autonomousSystemNumber = autonomousSystemNumber;
    this.autonomousSystemOrganization = autonomousSystemOrganization;
    this.ipAddress = ipAddress;
  }


  public AutonomousSystem(AsnResponse response) {
    this.autonomousSystemNumber = response.getAutonomousSystemNumber();
    this.autonomousSystemOrganization = response.getAutonomousSystemOrganization();
    this.ipAddress = response.getIpAddress();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AutonomousSystem that = (AutonomousSystem) o;
    return Objects.equals(autonomousSystemNumber, that.autonomousSystemNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(autonomousSystemNumber);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("autonomousSystemNumber", autonomousSystemNumber)
        .append("autonomousSystemOrganization", autonomousSystemOrganization)
        .append("ipAddress", ipAddress)
        .toString();
  }

  public Integer getAutonomousSystemNumber() {
    return autonomousSystemNumber;
  }

  public String getAutonomousSystemOrganization() {
    return autonomousSystemOrganization;
  }

  public String getIpAddress() {
    return ipAddress;
  }
}
