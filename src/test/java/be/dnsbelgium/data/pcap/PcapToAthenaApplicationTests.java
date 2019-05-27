package be.dnsbelgium.data.pcap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
@SpringBootTest
@TestPropertySource(locations = "classpath:test-application.properties")
public class PcapToAthenaApplicationTests {

	@Test
	public void contextLoads() {
	}

}
