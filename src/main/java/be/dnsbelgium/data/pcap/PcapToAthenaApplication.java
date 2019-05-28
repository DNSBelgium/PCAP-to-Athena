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
