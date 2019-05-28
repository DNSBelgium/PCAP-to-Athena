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
