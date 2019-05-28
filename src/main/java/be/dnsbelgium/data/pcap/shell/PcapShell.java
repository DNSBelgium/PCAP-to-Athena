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

package be.dnsbelgium.data.pcap.shell;

import be.dnsbelgium.data.pcap.aws.athena.AthenaTools;
import be.dnsbelgium.data.pcap.aws.s3.Downloader;
import be.dnsbelgium.data.pcap.aws.s3.S3PcapFile;
import be.dnsbelgium.data.pcap.aws.s3.Tagger;
import be.dnsbelgium.data.pcap.aws.s3.Uploader;
import be.dnsbelgium.data.pcap.convertor.PcapConvertor;
import be.dnsbelgium.data.pcap.reader.PcapFileReader;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.convertor.ConversionJob;
import be.dnsbelgium.data.pcap.convertor.ConvertorConfig;
import be.dnsbelgium.data.pcap.convertor.ConvertorService;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import be.dnsbelgium.data.pcap.utils.FileSize;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static be.dnsbelgium.data.pcap.convertor.ConvertorService.TAG_CONVERSION_STATUS;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unused")
@ShellComponent
public class PcapShell implements ApplicationContextAware {

  private String prefix = "";
  private static final Logger logger = getLogger(PcapShell.class);

  private final ConvertorConfig config;
  private final Downloader downloader;
  private final Tagger tagger;
  private final PcapConvertor pcapConvertor;
  private final FileHelper fileHelper;
  private final ConvertorService convertorService;
  private AthenaTools athena;

  private ApplicationContext applicationContext;

  @Autowired
  public PcapShell(ConvertorConfig config, Downloader downloader, Uploader uploader, Tagger tagger,
                   PcapConvertor pcapConvertor, FileHelper fileHelper, ConvertorService convertorService,
                   AthenaTools athena) {
    logger.info("****** creating a Shell *****");
    this.config = config;
    this.downloader = downloader;
    this.tagger = tagger;
    this.pcapConvertor = pcapConvertor;
    this.fileHelper = fileHelper;
    this.convertorService = convertorService;
    this.athena = athena;
  }

  @ShellMethod("show current config")
  public void showConfig() {
    logger.info("pcap.bucket.name = {}", config.getPcapBucketName());
    logger.info("serverNames = {}", config.getServerNames());
    logger.info("prefix = {}", prefix);
    convertorService.logConfig();
    pcapConvertor.logConfig();
  }

  @ShellMethod("specify the S3 prefix to apply from now on")
  public void setPrefix(@ShellOption(defaultValue="") String prefix) {
    this.prefix = prefix;
    logger.info("S3 prefix = {}", prefix);
  }

  @ShellMethod("list the S3 objects using current prefix")
  public void listObjects() {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    logger.info("Found {} files under {}", files.size(), prefix);
  }

  @ShellMethod("list the folders on S3 in using current prefix")
  public String listFolders() {
    List<String> folders = downloader.listFolders(config.getPcapBucketName(), prefix);
    logger.info("Number of folders found: {}", folders.size());
    StringBuilder output = new StringBuilder();
    for (String folder : folders) {
      output.append(folder);
      output.append("\n");
    }
    return output.toString();
  }

  @ShellMethod("list the files on S3 in using current prefix")
  public String listFiles(boolean details) {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    long totalSize = 0;
    StringBuilder output = new StringBuilder();
    for (S3PcapFile file : files) {
      if (details) {
        output.append(file.toString()).append("\n");
      }
      totalSize += file.getObjectSummary().getSize();
    }
    logger.info("Number of files found: {}", files.size());
    logger.info("Total size: {}", FileSize.friendlySize(totalSize));
    logger.info("Total size: {} GB", totalSize / FileSize.BYTES_PER_GB);
    return output.toString();
  }

  @ShellMethod("count the files on S3 in using current prefix")
  public int countFiles() {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    logger.info("Number of files found: {}", files.size());
    return files.size();
  }

  @ShellMethod("Exit the EU")
  public void brexit(boolean no_deal) {
    if (no_deal) {
      logger.info("Quitting without a deal");
      ((ConfigurableApplicationContext) applicationContext).close();
      throw new ExitRequest();
    }
    logger.info("Maybe you want a delay ?");
  }


  @SuppressWarnings("NullableProblems")
  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  private static class Counter {
    long bytes;
    long count;

    public Counter(long bytes, long count) {
      this.bytes = bytes;
      this.count = count;
    }
  }

  @ShellMethod("count files and fileSize grouped by conversion status")
  public String countPerStatus() {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    int totalBytes = 0;
    Map<String, Counter> countPerStatus = new HashMap<>();

    for (S3PcapFile file : files) {
      long bytes = file.getObjectSummary().getSize();
      totalBytes += bytes;
      Map<String, String> tags = tagger.getTags(file);
      String status = tags.get(TAG_CONVERSION_STATUS);
      if (status == null) {
        status = "NO_TAG";
      }
      Counter counter = countPerStatus.get(status);
      if (counter == null) {
        counter = new Counter(0,0);
        countPerStatus.put(status, counter);
      }
      counter.bytes += bytes;
      counter.count++;
    }
    StringBuilder builder = new StringBuilder();
    for (String status : countPerStatus.keySet()) {
      Counter counter = countPerStatus.get(status);
      long gb = counter.bytes / FileSize.BYTES_PER_GB;
      builder
          .append(status).append(" => ").append(counter.count).append(" files: ")
          .append(counter.bytes).append(" bytes = ").append(gb).append(" GB\n");
    }
    logger.info(builder.toString());
    return builder.toString();
  }

  @ShellMethod("Find all files using current prefix and update the CONVERSION_STATUS tag when it matches oldValue")
  public String updateConversionStatus(String oldValue, String newValue) {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    long totalBytes = 0;
    int count = 0;
    for (S3PcapFile file : files) {
      Map<String, String> tags = tagger.getTags(file);
      String status = tags.get(TAG_CONVERSION_STATUS);
      if (status == null) {
        status = "NO_TAG";
      }
      if (status.equals(oldValue)) {
        tagger.setTag(config.getPcapBucketName(), file.getKey(), TAG_CONVERSION_STATUS, newValue);
        count++;
        totalBytes += file.getObjectSummary().getSize();
      }
    }
    long gb = totalBytes / FileSize.BYTES_PER_GB;
    return String.format("Updated %d files (for a total of %d GB)", count, gb);
  }

  @ShellMethod("Find all files using current prefix and remove the CONVERSION_STATUS tag when it matches oldValue ")
  public String removeConversionStatus(String oldValue) {
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    long totalBytes = 0;
    int count = 0;
    for (S3PcapFile file : files) {
      Map<String, String> tags = tagger.getTags(file);
      String status = tags.get(TAG_CONVERSION_STATUS);
      if (status.equals(oldValue)) {
        tagger.removeTag(config.getPcapBucketName(), file.getKey(), TAG_CONVERSION_STATUS);
        count++;
        totalBytes += file.getObjectSummary().getSize();
      }
    }
    long gb = totalBytes / FileSize.BYTES_PER_GB;
    return String.format("Updated %d files (for a total of %d GB)", count, gb);
  }

  @ShellMethod("show thread info")
  public String thread() {
    logger.info("Thread.currentThread().getName() = {}", Thread.currentThread().getName());
    logger.info("Thread.activeCount() = {}", Thread.activeCount());
    Thread.dumpStack();
    return Thread.currentThread().getName();
  }

  @ShellMethod("process all PCAP files for given month and server")
  public long processMonth(int year, int month, String server) {
    long pcapBytes = 0;
    LocalDate date = LocalDate.of(year, month, 1);
    ServerInfo serverInfo = new ServerInfo(server + config.getServerSuffix(), server, server);
    logger.info("START processing all days of {} for serverInfo = {}", date.format(DateTimeFormatter.ofPattern("yyyy'-'MM")), serverInfo);

    while (date.getMonthValue() == month) {
      logger.info("date = {}", date);
      File parquetFolder = fileHelper.uniqueSubFolder(config.getParquetOutputFolder());
      ConversionJob job = new ConversionJob(serverInfo, date, parquetFolder);
      convertorService.execute(job);
      job.logStatus();
      date = date.plusDays(1);
      pcapBytes += job.getLocalConversionJob().getTotalPcapBytes();
      logger.info("Total PCAP files processed so far: {}", FileSize.friendlySize(pcapBytes));
    }
    logger.info("DONE processing all days of {}-{} for {}", year, month, serverInfo);
    logger.info("Total PCAP files processed for {} in {}/{} => {}", server, year, month, FileSize.friendlySize(pcapBytes));
    logger.info("==================================================");
    return pcapBytes;
  }

  @ShellMethod("process all PCAP files of all servers for given month")
  void processMonthForAllServers(int year, int month) {
    logger.info("Processing all PCAP files of {}/{} for all servers", year, month);
    long pcapBytes = 0;
    for (String serverName : config.getServerNames()) {
      pcapBytes += processMonth(year, month, serverName);
    }
    logger.info("Total PCAP files processed for {}/{} => {}", year, month, FileSize.friendlySize(pcapBytes));
    logger.info("==================================================");
  }

  @ShellMethod("process job")
  public String launchJob(String yyyy_mm_dd, String server) {
    ConversionJob job = createJob(yyyy_mm_dd, server);
    logger.info("Starting conversion job: {}", job);
    convertorService.execute(job);
    job.logStatus();
    return job.getStatus().toString();
  }

  private ConversionJob createJob(String yyyy_mm_dd, String server) {
    logger.info("yyyy_mm_dd = [{}]", yyyy_mm_dd);
    logger.info("server = [{}]", server);
    LocalDate date = LocalDate.parse(yyyy_mm_dd);
    ServerInfo serverInfo = new ServerInfo(server + config.getServerSuffix(), server, server);
    logger.info("=> date = {}", date);
    logger.info("=> serverInfo = {}", serverInfo);
    File parquetFolder = fileHelper.uniqueSubFolder(config.getParquetOutputFolder());
    return new ConversionJob(serverInfo, date, parquetFolder);
  }


  @ShellMethod("Analyze specified decoder-state file")
  public String loadDecoderState(String server, @ShellOption(defaultValue="20") int limit) {
    logger.info("server = {}", server);
    ServerInfo serverInfo = new ServerInfo(server + config.getServerSuffix(), server, server);
    PcapFileReader pcapFileReader = new PcapFileReader(null, serverInfo, Lists.newArrayList(), null);
    pcapFileReader.loadState();
    pcapFileReader.printState(limit);
    return "state loaded";
  }

  @ShellMethod("list all tables in given Athena database")
  public void listTables(String database) {
    List<String> tables = athena.getTables(database);
    for (String table : tables) {
      logger.info("table = {}", table);
    }
  }

  @ShellMethod("show partitions of given table")
  public void showPartitionsOf(String databaseName, String tableName) {
    List<String> partitions = athena.getPartitions(databaseName, tableName);
    Collections.sort(partitions);
    for (String partition : partitions) {
      logger.info("partition = {}", partition);
    }
    logger.info("Table {} has {} partitions", tableName, partitions.size());
  }

  @ShellMethod("show partitions of the configured Athena table")
  public void showPartitions() {
    String db = config.getAthenaDatabaseName();
    String table = config.getAthenaTableName();
    logger.info("retrieving partitions of {}.{}", db, table);
    List<String> partitions = athena.getPartitions(db, table);
    for (String partition : partitions) {
      logger.info("  partition = {}", partition);
    }
    logger.info("Table {}.{} has {} partitions", db, table, partitions.size());
  }

  @ShellMethod("This will force Athena to detect any new partitions (by scanning S3) and can take a long time.")
  public void detectNewPartitions(String databaseName, String tableName) {
    athena.detectNewPartitions(databaseName, tableName);
  }

  @ShellMethod("Add a new partition")
  public void addPartition(String dd_mm_yyyy, String server, @ShellOption(defaultValue = "") String s3Location) {
    if (s3Location.equals("")) {
      s3Location = config.getParquetS3Location();
    }
    LocalDate date = LocalDate.parse(dd_mm_yyyy, DateTimeFormatter.ofPattern("ddMMyyyy"));
    ServerInfo serverInfo = new ServerInfo(server, server, "");
    athena.addPartition(date, serverInfo, config.getAthenaDatabaseName(), config.getAthenaTableName(), s3Location);
  }

  @ShellMethod("Count the rows in the Athena table with DNS queries")
  public String countRows() {
    long count = athena.countRows(config.getAthenaDatabaseName(), config.getAthenaTableName());
    String msg = String.format("Found %,d rows in table %s", count, config.getAthenaTableName());
    logger.info(msg);
    return msg;
  }

  @ShellMethod("list all PCAP files from all servers for given day")
  public void listFilesForDay(String yyyy_mm_dd) {
    if (yyyy_mm_dd.length() != 10) {
      logger.error("Invalid date. Use yyyy_mm_dd format");
      return;
    }
    LocalDate date = LocalDate.parse(yyyy_mm_dd, DateTimeFormatter.ofPattern("yyyy'_'MM'_'dd"));
    convertorService.listFiles(date);
  }

}
