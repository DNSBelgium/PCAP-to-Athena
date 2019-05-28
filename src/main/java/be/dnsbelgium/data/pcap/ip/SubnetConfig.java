/*
 * This file is part of PCAP to Athena.
 *
 * Copyright (c) 2019 DNS Belgium.
 *
 * PCAP to Athena is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PCAP to Athena is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PCAP to Athena.  If not, see <https://www.gnu.org/licenses/>.
 */

package be.dnsbelgium.data.pcap.ip;

import be.dnsbelgium.data.pcap.utils.GeoLookupUtil;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@ComponentScan
public class SubnetConfig {

  private GoogleSubnetFetcher googleSubnetFetcher;
  private CloudFlareSubnetFetcher cloudFlareSubnetFetcher;
  private OpenDNSSubnetFetcher openDNSSubnetFetcher;
  private Quad9SubnetFetcher quad9SubnetFetcher;

  @Value("${resolver.subnets.folder}")
  private String resolverDataFolder;

  @Value("${geoIP.maxmind.folder}")
  private String geoLookupFolder;

  private static final Logger logger = getLogger(SubnetConfig.class);

  @Autowired
  public SubnetConfig(GoogleSubnetFetcher googleSubnetFetcher, CloudFlareSubnetFetcher cloudFlareSubnetFetcher,
                      OpenDNSSubnetFetcher openDNSSubnetFetcher, Quad9SubnetFetcher quad9SubnetFetcher) {
    this.googleSubnetFetcher = googleSubnetFetcher;
    this.cloudFlareSubnetFetcher = cloudFlareSubnetFetcher;
    this.openDNSSubnetFetcher = openDNSSubnetFetcher;
    this.quad9SubnetFetcher = quad9SubnetFetcher;
  }

  @Bean
  public GeoLookupUtil geoLookupUtil() {
    logger.info("creating a GeoLookupUtil object with geoLookupFolder = {}", geoLookupFolder);
    return new GeoLookupUtil(geoLookupFolder);
  }

  @Bean
  SubnetChecks subnetChecks() throws IOException {
    logger.info("creating folder {}", resolverDataFolder);
    Files.createDirectories(new File(resolverDataFolder).toPath());
    SubnetChecks checks = new SubnetChecks();
    checks.add("is_google", make("google-resolvers", googleSubnetFetcher));
    checks.add("is_cloudflare", make("cloudflare-resolvers", cloudFlareSubnetFetcher));
    checks.add("is_opendns", make("opendns-resolvers", openDNSSubnetFetcher));
    checks.add("is_quad9", make("quad9-resolvers", quad9SubnetFetcher));
    checks.updateAll();
    return checks;
  }

  private SubnetCheck make(String fileName, SubnetFetcher fetcher) {
    return new SubnetCheck(new File(resolverDataFolder, fileName), fetcher);
  }

}
