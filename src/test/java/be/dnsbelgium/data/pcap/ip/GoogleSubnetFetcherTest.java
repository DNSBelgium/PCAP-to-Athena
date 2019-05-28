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

public class GoogleSubnetFetcherTest {

  /**
   * This test requires access to external DNS
   */
  @Test
  public void fetchSubnets() {
    GoogleSubnetFetcher subnetFetcher = new GoogleSubnetFetcher("locations.publicdns.goog.", 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertTrue("we should find at least 5 google subnets", subnets.size() >= 5);
  }

  @Test
  public void wrongHostname() {
    GoogleSubnetFetcher subnetFetcher = new GoogleSubnetFetcher("this-does-not-exist-probably.goog", 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertEquals("We should find 0 Google subnets", 0,  subnets.size());
  }

  @Test
  public void illegalHostname() {
    GoogleSubnetFetcher subnetFetcher = new GoogleSubnetFetcher("@t#$..%goog", 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertEquals("We should find 0 Google subnets", 0,  subnets.size());
  }

}