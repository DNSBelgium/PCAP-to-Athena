package be.dnsbelgium.data.pcap.utils;

import be.dnsbelgium.data.pcap.PcapToAthenaApplication;
import be.dnsbelgium.data.pcap.ip.Quad9SubnetFetcher;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = PcapToAthenaApplication.class)
@TestPropertySource(locations = "classpath:test-application.properties")
public class GeoLookupUtilTest {

  private GeoLookupUtil geoLookup;

  private static final Logger logger = getLogger(GeoLookupUtilTest.class);

  @Value("${geoIP.maxmind.folder}")
  private String folder;

  @Before
  public void before() {
    logger.info("Loading Maxmind database from folder {}", folder);
    geoLookup = new GeoLookupUtil(folder);
  }

  @Test
  public void lookup() {
    String ip = "81.164.126.240";
    assertEquals("BE", geoLookup.lookupCountry(ip));
    assertEquals("6848", geoLookup.lookupASN(ip));

    ip = "8.8.8.8";
    assertEquals("US", geoLookup.lookupCountry(ip));
    assertEquals("15169", geoLookup.lookupASN(ip));

    ip = "212.114.98.233";
    assertEquals("NL", geoLookup.lookupCountry(ip));
    assertEquals("12859", geoLookup.lookupASN(ip));

    ip = "74.80.115.0";
    assertEquals("CN", geoLookup.lookupCountry(ip));
    assertEquals("715", geoLookup.lookupASN(ip));

    ip = "74.80.89.0";
    assertEquals("DE", geoLookup.lookupCountry(ip));
    assertEquals("715", geoLookup.lookupASN(ip));

    ip = "2620:0171:00F7:0000::";
    assertEquals("ZA", geoLookup.lookupCountry(ip));
    assertEquals("42", geoLookup.lookupASN(ip));

    ip = "2001:0500:0015:0600::";
    assertEquals("US", geoLookup.lookupCountry(ip));
    assertEquals("715", geoLookup.lookupASN(ip));
  }

  @Test
  public void lookupBytes() throws UnknownHostException {
    // byte[] addrBytes
    InetAddress address = InetAddress.getByName("74.80.89.0");
    assertEquals("DE", geoLookup.lookupCountry(address));
    assertEquals("715", geoLookup.lookupASN(address));
  }

  @Test
  public void printAllQuad9() throws IOException {
    Quad9SubnetFetcher quad9SubnetFetcher = new Quad9SubnetFetcher();
    List<String> subnets = quad9SubnetFetcher.fetchSubnets();

    int countryFoundCounter = 0;
    int countryNotFoundCounter = 0;

    int cityFoundCounter = 0;
    int cityNotFoundCounter = 0;
    int asnFoundCounter = 0;
    int asnNotFoundCounter = 0;
    Set<String> cities = new HashSet<>();
    Set<String> asnSet = new HashSet<>();
    Set<String> countries = new HashSet<>();


    for (String subnet : subnets) {
      String ip = subnet.substring(0, subnet.length()-3);
      String country = geoLookup.lookupCountry(ip);
      String asn = geoLookup.lookupASN(ip);

//      if (ip.contains(":")) {
//        System.out.println("IP: " + ip);
//        System.out.println("  asn = " + asn);
//        System.out.println("  city = " + city);
//        System.out.println("  country = " + country);
//      }

      if (country != null) {
        countryFoundCounter++;
        countries.add(country);
      } else {
        countryNotFoundCounter++;
      }
      if (asn != null) {
        asnFoundCounter++;
        asnSet.add(asn);
      } else {
        asnNotFoundCounter++;
      }
    }

    logger.info("countryFoundCounter = {}", countryFoundCounter);
    logger.info("countryNotFoundCounter = {}", countryNotFoundCounter);
    logger.info("cityFoundCounter = {}", cityFoundCounter);
    logger.info("cityNotFoundCounter = {}", cityNotFoundCounter);
    logger.info("asnFoundCounter = {}", asnFoundCounter);
    logger.info("asnNotFoundCounter = {}", asnNotFoundCounter);
    logger.info("cities = {}", cities.size());
    logger.info("cities = {}", cities);
    logger.info("countries = {}", countries.size());
    logger.info("countries = {}", countries);
    logger.info("asnSet = {}", asnSet.size());
    logger.info("asnSet = {}", asnSet);

  }

  @Test
  public void testLookupAsn() throws UnknownHostException {
    for (String ip : Lists.newArrayList("185.20.63.0", "2001:0500:0015:0600::")) {
      InetAddress address = InetAddress.getByName(ip);

      String byString = geoLookup.lookupASN(ip);
      String byAddress = geoLookup.lookupASN(address);

      System.out.println("byString = " + byString);
      System.out.println("byAddress = " + byAddress);

      assertEquals(byAddress, byString);
    }
  }

  @Test
  public void testLookupCountry() throws UnknownHostException {
    String ip = "185.20.63.0";
    InetAddress address = InetAddress.getByName(ip);

    String byString = geoLookup.lookupCountry(ip);
    String byAddress = geoLookup.lookupCountry(address);

    System.out.println("byString = " + byString);
    System.out.println("byAddress = " + byAddress);
  }

}