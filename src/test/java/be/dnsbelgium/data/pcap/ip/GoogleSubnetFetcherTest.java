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