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
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.slf4j.LoggerFactory.getLogger;

public class SubnetTest {

  private static final Logger logger = getLogger(SubnetTest.class);

  @Test
  public void testConstructor() throws UnknownHostException {
    Subnet subnet = new Subnet(InetAddress.getByName("10.20.30.40"), 16);
    logger.info("subnet = {}", subnet);
    assertEquals("10.20.0.0/255.255.0.0", subnet.toString());
    Subnet subnet2 = new Subnet(InetAddress.getByName("10.20.30.00"), 16);
    assertEquals(subnet, subnet2);
    Subnet subnet3 = new Subnet(InetAddress.getByName("10.20.00.00"), 16);
    assertEquals(subnet, subnet3);
    Subnet subnet4 = new Subnet(InetAddress.getByName("10.21.00.00"), 16);
    assertNotEquals(subnet, subnet4);
    Subnet subnet5 = new Subnet(InetAddress.getByName("10.20.0.0"), InetAddress.getByName("255.255.0.0"));
    assertEquals(subnet, subnet5);
    assertEquals(subnet, Subnet.createInstance("10.20.0.0/16"));
    assertEquals("10.20.30.40/255.255.255.255", Subnet.createInstance("10.20.30.40").toString());
  }

}