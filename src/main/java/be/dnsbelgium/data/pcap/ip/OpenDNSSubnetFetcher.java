package be.dnsbelgium.data.pcap.ip;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class OpenDNSSubnetFetcher implements SubnetFetcher {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String url;

  private int timeOutInSeconds;

  private static final Logger logger = getLogger(OpenDNSSubnetFetcher.class);

  @Autowired
  public OpenDNSSubnetFetcher(@Value("${opendns.resolver.url}") String url,
                              @Value("${opendns.fetcher.timeOutInSeconds}") int timeOutInSeconds) {
    this.url = url;
    this.timeOutInSeconds = timeOutInSeconds;
  }

  @Override
  public List<String> fetchSubnets() {
    logger.info("Fetch OpenDNS resolver addresses from url: {}", url);

    List<String> subnets = new ArrayList<>();

    int timeoutInMillis = timeOutInSeconds * 1000;
    RequestConfig config = RequestConfig
        .custom()
        .setConnectTimeout(timeoutInMillis)
        .setConnectionRequestTimeout(timeoutInMillis)
        .setSocketTimeout(timeoutInMillis)
        .build();

    CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

    try {
      HttpGet get = new HttpGet(url);
      CloseableHttpResponse response = client.execute(get);

      @SuppressWarnings("unchecked")
      List<Map<String, String>> locations =
          objectMapper.readValue(response.getEntity().getContent(), List.class);

      for (Map<String, String> location : locations) {
        String v4 = location.get("subnetV4");
        String v6 = location.get("subnetV6");
        logger.debug("Add OpenDNS resolver IP ranges, subnetV4: " + v4 + " subnetV6: " + v6);
        subnets.add(v4);
        subnets.add(v6);
      }

    } catch (Exception e) {
      logger.error("Problem while fetching OpenDns subnets.", e);
    } finally {
      IOUtils.closeQuietly(client);
    }
    logger.info("Fetched {} OpenDNS subnets", subnets.size());
    return subnets;
  }
}
