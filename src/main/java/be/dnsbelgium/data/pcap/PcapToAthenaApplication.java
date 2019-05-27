package be.dnsbelgium.data.pcap;

import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration;

import static org.slf4j.LoggerFactory.getLogger;

@SpringBootApplication(exclude = { ContextInstanceDataAutoConfiguration.class } )
public class PcapToAthenaApplication {

	private static final Logger logger = getLogger(PcapToAthenaApplication.class);

	public static void main(String[] args) {

		logSystemProperty("user.name");
		logSystemProperty("user.home");
		logSystemProperty("user.dir");
		logSystemProperty("java.home");
		logSystemProperty("java.vendor");
		logSystemProperty("java.vendor.url");
		logSystemProperty("java.version");
		logSystemProperty("os.arch");
		logSystemProperty("os.name");
		logSystemProperty("os.version");
		logSystemProperty("path.separator");

		String[] paths = System.getProperty("java.class.path").split(":");
		for (String path : paths) {
			logger.info("classpath item: {}", path);
		}


		SpringApplication.run(PcapToAthenaApplication.class, args);
	}

	public static void logSystemProperty(String name) {
		logger.info("{}=[{}]", name, System.getProperty(name));
	}

}
