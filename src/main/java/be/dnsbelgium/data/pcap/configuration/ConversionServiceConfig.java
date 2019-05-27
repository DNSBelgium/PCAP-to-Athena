package be.dnsbelgium.data.pcap.configuration;

import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
public class ConversionServiceConfig {

  private static final Logger logger = getLogger(ConversionServiceConfig.class);

  // Need to split comma-separated property value
  @Bean
  public ConversionService conversionService() {
    logger.info("creating a DefaultConversionService");
    return new DefaultConversionService();
  }

}
