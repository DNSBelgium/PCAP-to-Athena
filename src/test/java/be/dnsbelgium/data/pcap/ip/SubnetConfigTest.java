package be.dnsbelgium.data.pcap.ip;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {SubnetConfig.class}, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(locations="classpath:test-application.properties")
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