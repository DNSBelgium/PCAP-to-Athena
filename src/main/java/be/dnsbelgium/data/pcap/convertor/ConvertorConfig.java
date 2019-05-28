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

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class ConvertorConfig {

  private final boolean pcapFoldersNewStyle;
  private String parquetOutputFolder;
  private String serverSuffix;
  private boolean deletePcapAfterConversion;
  private boolean deleteParquetAfterUpload;
  private String pcapBucketName;
  private String parquetBucketName;
  private String archiveBucketName;
  private final String archivePrefix;
  private final String parquetPrefix;
  private final String parquetRepoName;
  private final String pcapDownloadFolder;
  private List<String> serverNames;
  private String athenaDatabaseName;
  private String athenaTableName;


  private static final Logger logger = getLogger(ConvertorConfig.class);

  @Autowired
  public ConvertorConfig(
      @Value("${pcap.bucket.name}") String pcapBucketName,
      @Value("${parquet.bucket.name}") String parquetBucketName,
      @Value("${pcap.archive.bucket.name}") String archiveBucketName,
      @Value("${pcap.archive.prefix}") String archivePrefix,
      @Value("${pcap.folders.newStyle}") boolean pcapFoldersNewStyle,
      @Value("${pcap.download.folder}") String pcapDownloadFolder,
      @Value("${parquet.output.folder}") String parquetOutputFolder,
      @Value("${server.suffix}")   String serverSuffix,
      @Value("${pcap.delete.after.conversion}")  boolean deletePcapAfterConversion,
      @Value("${parquet.delete.after.upload}") boolean deleteParquetAfterUpload,
      @Value("${parquet.prefix}") String parquetPrefix,
      @Value("${parquet.repo.name}") String parquetRepoName,
      @Value("${serverNames}") List<String> serverNames,
      @Value("${athena.database.name}") String athenaDatabaseName,
      @Value("${athena.table.name}") String athenaTableName
  ) throws IOException {
    this.parquetOutputFolder = parquetOutputFolder;
    this.serverSuffix = serverSuffix;
    this.deletePcapAfterConversion = deletePcapAfterConversion;
    this.deleteParquetAfterUpload = deleteParquetAfterUpload;
    this.pcapBucketName = pcapBucketName;
    this.parquetBucketName = parquetBucketName;
    this.archiveBucketName = archiveBucketName;
    this.archivePrefix = appendTrailingSlash(archivePrefix);
    this.parquetPrefix = appendTrailingSlash(parquetPrefix);
    this.pcapDownloadFolder = pcapDownloadFolder;
    this.serverNames = serverNames;
    this.athenaDatabaseName = athenaDatabaseName;
    this.athenaTableName = athenaTableName;
    this.pcapFoldersNewStyle = pcapFoldersNewStyle;
    this.parquetRepoName = parquetRepoName;

    // Check configuration

    Path path = Paths.get(this.pcapDownloadFolder);
    if (!Files.exists(path)) {
      try {
        Files.createDirectory(path);
      } catch (IOException e) {
        logger.error("{} does not exists or it is not accessible", path);
        throw e;
      }
    }

    path = Paths.get(this.parquetOutputFolder);
    if (!Files.exists(path)) {
      try {
        Files.createDirectory(path);
      } catch (IOException e) {
        logger.error("{} does not exists or it is not accessible", path);
        throw e;
      }
    }

  }

  private String appendTrailingSlash(String input) {
    if (input != null && input.length() > 0 && !input.endsWith("/")) {
      return input + "/";
    } else {
      return input;
    }
  }

  @PostConstruct
  public void logConfig() {
    logger.info("   ${pcap.bucket.name}             = {}", pcapBucketName);
    logger.info("   ${pcap.folders.newStyle}        = {}", pcapFoldersNewStyle);
    logger.info("   ${pcap.download.folder}         = {}", pcapDownloadFolder);
    logger.info("   ${parquet.output.folder}        = {}", parquetOutputFolder);
    logger.info("   ${parquet.bucket.name}          = {}", parquetBucketName);
    logger.info("   ${parquet.prefix}               = {}", parquetPrefix);
    logger.info("   ${parquet.repo.name}            = {}", parquetRepoName);
    logger.info("   ${pcap.archive.bucket.name}     = {}", archiveBucketName);
    logger.info("   ${pcap.archive.prefix}          = {}", archivePrefix);
    logger.info("   ${server.suffix}                = {}", serverSuffix);
    logger.info("   ${pcap.delete.after.conversion} = {}", deletePcapAfterConversion);
    logger.info("   ${parquet.delete.after.upload}  = {}", deleteParquetAfterUpload);
    logger.info("   ${athena.database.name}         = {}", athenaDatabaseName);
    logger.info("   ${athena.table.name}            = {}", athenaTableName);
  }

  public String getParquetS3Location() {
    return "s3://" + parquetBucketName + "/" + parquetPrefix + "/" + parquetRepoName + "/".replaceAll("//", "/");
  }

  public String getParquetRepoName() {
    return parquetRepoName;
  }

  public String getParquetOutputFolder() {
    return parquetOutputFolder;
  }

  public String getServerSuffix() {
    return serverSuffix;
  }

  public boolean isDeletePcapAfterConversion() {
    return deletePcapAfterConversion;
  }

  public String getPcapBucketName() {
    return pcapBucketName;
  }

  public String getParquetBucketName() {
    return parquetBucketName;
  }

  public String getParquetPrefix() {
    return parquetPrefix;
  }

  public String getArchiveBucketName() {
    return archiveBucketName;
  }

  public String getArchivePrefix() {
    return archivePrefix;
  }

  public String getPcapDownloadFolder() {
    return pcapDownloadFolder;
  }

  public List<String> getServerNames() {
    return serverNames;
  }

  public String getAthenaDatabaseName() {
    return athenaDatabaseName;
  }

  public String getAthenaTableName() {
    return athenaTableName;
  }

  public boolean isPcapFoldersNewStyle() {
    return pcapFoldersNewStyle;
  }
}
