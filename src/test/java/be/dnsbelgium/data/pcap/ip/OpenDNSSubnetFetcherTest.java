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

package be.dnsbelgium.data.pcap.ip;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OpenDNSSubnetFetcherTest {

  String URL = "https://www.opendns.com/network-map-data";

  @Test
  public void fetchSubnets() {
    OpenDNSSubnetFetcher subnetFetcher = new OpenDNSSubnetFetcher(URL, 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertTrue("We should find at least 5 OpenDNS subnets",  subnets.size() > 5);
  }

  @Test
  public void wrongURL() {
    OpenDNSSubnetFetcher subnetFetcher = new OpenDNSSubnetFetcher("https://www.opendns.com/this-does-not-exist-probably", 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertEquals("We should find 0 OpenDNS subnets", 0,  subnets.size());
  }

  @Test
  public void wrongHost()  {
    OpenDNSSubnetFetcher subnetFetcher = new OpenDNSSubnetFetcher("https://www.example.com/host-does-not-exist", 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertEquals("We should find 0 OpenDNS subnets", 0,  subnets.size());
  }


}
