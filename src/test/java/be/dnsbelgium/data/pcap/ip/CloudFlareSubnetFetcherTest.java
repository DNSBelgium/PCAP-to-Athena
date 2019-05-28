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

public class CloudFlareSubnetFetcherTest {


  String URL_V4 = "https://www.cloudflare.com/ips-v4";
  String URL_V6 = "https://www.cloudflare.com/ips-v6";

  /**
   * This test requires access to https://www.cloudflare.com
   */
  @Test
  public void fetchSubnets() {
    CloudFlareSubnetFetcher subnetFetcher = new CloudFlareSubnetFetcher(URL_V4, URL_V6, 15);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertTrue("we should find at least 5 cloudflare subnets", subnets.size() >= 5);
  }

  @Test
  public void wrongURL() {
    CloudFlareSubnetFetcher subnetFetcher = new CloudFlareSubnetFetcher("https://example/does-not-exist", "https://example/404", 1);
    List<String> subnets = subnetFetcher.fetchSubnets();
    assertTrue("we should find no cloudflare subnets but found " + subnets, subnets.isEmpty());
    assertEquals(0, subnets.size());
  }
}