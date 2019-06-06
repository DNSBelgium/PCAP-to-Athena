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
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {SubnetConfig.class}, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(locations="classpath:test-application.properties")
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
public class SubnetConfigTest {

  @Autowired
  SubnetChecks checks;

  private static final Logger logger = getLogger(SubnetConfigTest.class);

  @Test
  public void subnetChecks() {
    assertNotNull("checks should be injected", checks);

    assertEquals("we currently know the ranges of 4 public DNS providers", 4, checks.getColumns().size());

    for (String column : checks.getColumns()) {
      SubnetCheck check = checks.get(column);
      assertTrue(column + " should have at least one subnet", check.getSubnetCount() > 0);
      assertTrue("file " +check.getFile() + " should now exist", check.getFile().exists());

      boolean deleted =  check.getFile().delete();
      logger.info("deleted {} : {}", check.getFile(), deleted);
      assertTrue("We should be able to delete " + check.getFile(), deleted);

    }
  }
}
