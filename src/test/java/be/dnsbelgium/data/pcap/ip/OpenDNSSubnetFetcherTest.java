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
