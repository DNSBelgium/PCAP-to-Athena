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

package be.dnsbelgium.data.pcap.model;

public class ServerInfo extends nl.sidn.pcap.support.ServerInfo {

  public ServerInfo(String fullname) {
    super();
    this.setFullname(fullname);
    this.setName(fullname);
    this.setLocation(fullname);
  }

  public ServerInfo(String fullname, String name, String location) {
    super();
    this.setFullname(fullname);
    this.setName(name);
    this.setLocation(location);
  }

  public String getNormalizedServerName() {
    return getFullname().replaceAll("[^A-Za-z0-9 ]", "_");
  }

  @Override
  public String toString() {
    return "ServerInfo{" +
        "fullname='" + this.getFullname() + '\'' +
        '}';
  }

}
