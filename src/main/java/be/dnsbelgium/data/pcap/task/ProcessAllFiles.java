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

package be.dnsbelgium.data.pcap.task;

import be.dnsbelgium.data.pcap.aws.s3.Downloader;
import be.dnsbelgium.data.pcap.aws.s3.S3PcapFile;
import be.dnsbelgium.data.pcap.convertor.ConversionJob;
import be.dnsbelgium.data.pcap.convertor.ConvertorConfig;
import be.dnsbelgium.data.pcap.convertor.ConvertorService;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import org.slf4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static org.slf4j.LoggerFactory.getLogger;

@Component
@ConditionalOnProperty(value = "spring.shell.interactive.enabled", havingValue = "false")
public class ProcessAllFiles implements CommandLineRunner {

  private static final Logger logger = getLogger(ProcessAllFiles.class);

  private final Downloader downloader;
  private final ConvertorConfig convertorConfig;
  private final FileHelper fileHelper;
  private final ConvertorService convertorService;

  public ProcessAllFiles(Downloader downloader, ConvertorConfig convertorConfig, FileHelper fileHelper, ConvertorService convertorService) {
    this.downloader = downloader;
    this.convertorConfig = convertorConfig;
    this.fileHelper = fileHelper;
    this.convertorService = convertorService;
  }

  @Override
  public void run(String... args) {
    List<S3PcapFile> s3PcapFiles = downloader.listFilesIn(convertorConfig.getPcapBucketName(), convertorConfig.getPcapBucketPrefix());

    Map<String, Set<LocalDate>> datesPerServer = s3PcapFiles.stream()
        .collect(groupingBy(S3PcapFile::getServer, mapping(S3PcapFile::getDate, Collectors.toSet())));

    datesPerServer.entrySet().stream()
        .filter(entry -> isServerIncluded(entry.getKey()))
        .filter(entry -> isServerNotExcluded(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream().map(date -> toConversionJob(entry.getKey(), date)))
        .forEach(convertorService::execute);
  }

  private boolean isServerIncluded(String fullServerName) {
    logger.info("only including servers {}", convertorConfig.getIncludedServers());
    return convertorConfig.getIncludedServers().isEmpty()
        || convertorConfig.getIncludedServers().stream().map(it -> it + convertorConfig.getServerSuffix()).anyMatch(it -> it.equals(fullServerName));
  }

  private boolean isServerNotExcluded(String fullServerName) {
    logger.info("excluding servers {}", convertorConfig.getExcludedServers());
    return convertorConfig.getExcludedServers().stream().map(it -> it + convertorConfig.getServerSuffix()).noneMatch(it -> it.equals(fullServerName));
  }

  private ConversionJob toConversionJob(String server, LocalDate date) {
    ServerInfo serverInfo = new ServerInfo(server + convertorConfig.getServerSuffix(), server, server);
    File parquetFolder = fileHelper.uniqueSubFolder(convertorConfig.getParquetOutputFolder());
    return new ConversionJob(serverInfo, date, parquetFolder);
  }

}
