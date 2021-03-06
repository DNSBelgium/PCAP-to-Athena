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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static org.junit.Assert.*;

public class Quad9SubnetFetcherTest {

  @Test
  public void fetchSubnets() throws IOException {
    Quad9SubnetFetcher subnetFetcher = new Quad9SubnetFetcher();
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertTrue("we should find at least 5 quad9 subnets but was " + subnets.size(), subnets.size() >= 5);
    for (String subnet : subnets) {
      try {
        Subnet s = Subnet.createInstance(subnet);
        assertNotNull(s);
      } catch (UnknownHostException e) {
        fail(subnet + " => " + e.getMessage());
      }
    }
  }


}