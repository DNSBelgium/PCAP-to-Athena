/*
 * This file is part of PCAP to Athena.
 *
 * Copyright (c) 2019 DNS Belgium.
 *
 * PCAP to Athena is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PCAP to Athena is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PCAP to Athena.  If not, see <https://www.gnu.org/licenses/>.
 */

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
