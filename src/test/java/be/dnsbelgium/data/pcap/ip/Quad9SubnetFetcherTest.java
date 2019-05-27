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