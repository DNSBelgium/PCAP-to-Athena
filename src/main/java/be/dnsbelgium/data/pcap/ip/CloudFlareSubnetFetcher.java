package be.dnsbelgium.data.pcap.ip;

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
import org.springframework.util.StreamUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class CloudFlareSubnetFetcher implements SubnetFetcher {

  private static final Logger logger = getLogger(CloudFlareSubnetFetcher.class);

  private String ipv4_url;
  private String ipv6_url;
  private int timeOutInSeconds;

  @Autowired
  public CloudFlareSubnetFetcher(@Value("${cloudflare.subnets.ipv4.url}") String ipv4_url,
                                 @Value("${cloudflare.subnets.ipv6.url}") String ipv6_url,
                                 @Value("${cloudflare.subnets.timeOutInSeconds}") int timeOutInSeconds) {
    this.ipv4_url = ipv4_url;
    this.ipv6_url = ipv6_url;
    this.timeOutInSeconds = timeOutInSeconds;
  }

  @PostConstruct
  public void logSettings() {
    logger.info("ipv4_url = {}", ipv4_url);
    logger.info("ipv6_url = {}", ipv6_url);
  }

  @Override
  public List<String> fetchSubnets() {
    int timeoutInMillis = timeOutInSeconds * 1000;
    RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(timeoutInMillis)
        .setConnectionRequestTimeout(timeoutInMillis)
        .setSocketTimeout(timeoutInMillis)
        .build();
    CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    List<String> subnets = new ArrayList<>();

    try {
      process(client, ipv4_url, subnets);
      process(client, ipv6_url, subnets);
    } catch (Exception e) {
      logger.error("Problem while adding CloudFlare resolvers.", e);
    } finally {
      IOUtils.closeQuietly(client);
    }
    logger.info("Fetched {} subnets", subnets.size());
    return subnets;
  }


  private void process(CloseableHttpClient client, String url, List<String> subnets) throws IOException {
    logger.info("Fetching CloudFlare resolver addresses from url: " + url);
    HttpGet get = new HttpGet(url);
    try (CloseableHttpResponse response = client.execute(get)) {
      String content = StreamUtils.copyToString(response.getEntity().getContent(), Charset.forName("UTF-8"));
      logger.debug("content: {}", content);
      String[] addresses = content.split("\n");
      for (String subnet : addresses) {
        logger.debug("Add CloudFlare resolver IP range: {}", subnet);
        subnets.add(subnet);
      }
    }
  }


}
