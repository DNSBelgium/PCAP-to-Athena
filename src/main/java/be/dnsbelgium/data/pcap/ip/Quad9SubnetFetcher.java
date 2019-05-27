package be.dnsbelgium.data.pcap.ip;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class Quad9SubnetFetcher implements SubnetFetcher {

  private static final Logger logger = getLogger(Quad9SubnetFetcher.class);

  @Override
  public List<String> fetchSubnets() throws IOException {
    logger.info("Quad9 currently does not support fetching their subnets programmatically. You need to ask it via mail");
    return readFromClassPath();
  }

  public List<String> readFromClassPath() throws IOException {
    ClassPathResource resource = new ClassPathResource("quad9-resolvers.txt");
    logger.info("reading subnets from {}", resource);

    try (InputStream is = resource.getInputStream()) {
      List<String> lines = IOUtils.readLines(is);
      List<String> result = new ArrayList<>();

      for (String line : lines) {
        line = line.replaceAll(" ", "");
        if (!line.contains("/"))  {
          line = line + "/32";
        }
        result.add(line);
      }
      logger.info("Found {} quad9 subnets", result.size());
      return result;
    }
  }

}
