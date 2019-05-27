package be.dnsbelgium.data.pcap.ip;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xbill.DNS.*;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class GoogleSubnetFetcher implements SubnetFetcher {

  private String hostname;
  private int timeOutInSeconds;

  private static final Logger logger = getLogger(GoogleSubnetFetcher.class);

  public GoogleSubnetFetcher(
      @Value("${google.resolver.hostname}") String hostname,
      @Value("${google.resolver.timeOutInSeconds:15}") int timeOutInSeconds) {
    this.hostname = hostname;
    this.timeOutInSeconds = timeOutInSeconds;
  }

  @PostConstruct
  public void logSettings() {
    logger.info("hostname = {}", hostname);
    logger.info("timeOutInSeconds = {}", timeOutInSeconds);
  }

  @Override
  public List<String> fetchSubnets() {
    logger.info("retrieving list of Google resolvers by fetching TXT record of {}", hostname);
    List<String> subnets = new ArrayList<>();
    try {
      Resolver resolver = new SimpleResolver();
      // dns resolvers may take a long time to return a response.
      resolver.setTimeout(timeOutInSeconds);
      Lookup lookup = new Lookup(StringUtils.endsWith(hostname, ".") ? hostname : hostname + ".", Type.TXT);
      // always make sure the cache is empty
      lookup.setCache(new Cache());
      lookup.setResolver(resolver);
      Record[] records = lookup.run();
      if (records != null && records.length > 0) {
        parse(records[0], subnets);
      } else {
        logger.error("Found no records in DNS response");
      }
    } catch (Exception e) {
      logger.error("Problem while fetching Google resolvers.", e);
    }
    logger.info("Fetched {} subnets of Google Public DNS", subnets.size());
    return subnets;
  }

  private void parse(Record record, List<String> subnets) {
    TXTRecord txt = (TXTRecord) record;
    @SuppressWarnings("unchecked")
    List<String> lines = txt.getStrings();
    for (String line : lines) {
      String[] parts = StringUtils.split(line, " ");
      if (parts.length == 2) {
        logger.debug("Add Google resolver IP range: " + parts[0]);
        subnets.add(parts[0]);
      }
    }
  }

}
