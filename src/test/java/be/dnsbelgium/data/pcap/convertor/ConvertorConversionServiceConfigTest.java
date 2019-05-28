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

package be.dnsbelgium.data.pcap.convertor;

import be.dnsbelgium.data.pcap.configuration.ConversionServiceConfig;
import be.dnsbelgium.data.pcap.convertor.ConvertorConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = {ConvertorConfig.class, ConversionServiceConfig.class}
)
@EnableAutoConfiguration(exclude={
    DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration.class,
})
@TestPropertySource(locations="classpath:test-application.properties")
public class ConvertorConversionServiceConfigTest {

  @Autowired
  ConvertorConfig config;

  @Test
  public void testConfig() {
    String archive = config.getArchiveBucketName();
    System.out.println("archive = " + archive);
    System.out.println("serverNames = " + config.getServerNames().size());
    System.out.println("serverNames = " + config.getServerNames());
    for (String serverName : config.getServerNames()) {
      System.out.println("serverName = " + serverName);
    }
    assertEquals(3, config.getServerNames().size());
    assertTrue(config.getServerNames().contains("donald"));
    assertTrue(config.getServerNames().contains("mickey"));
    assertTrue(config.getServerNames().contains("pluto"));
  }


}