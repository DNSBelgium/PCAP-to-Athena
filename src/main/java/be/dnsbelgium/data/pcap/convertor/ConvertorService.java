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

import be.dnsbelgium.data.pcap.aws.athena.AthenaTools;
import be.dnsbelgium.data.pcap.aws.s3.*;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import be.dnsbelgium.data.pcap.utils.FileSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ConvertorService {

  private static final Logger logger = LoggerFactory.getLogger(ConvertorService.class);

  private ConvertorConfig config;

  private final Downloader downloader;
  private final Uploader uploader;
  private final Tagger tagger;
  private final PcapConvertor pcapConvertor;
  private final FileHelper fileHelper;
  private final AthenaTools athena;

  public final static String TAG_CONVERSION_STATUS = "CONVERSION_STATUS";
  public final static String TAG_CONVERSION_STARTED = "CONVERSION_STARTED";
  public final static String TAG_CONVERSION_ENDED = "CONVERSION_ENDED";
  public final static String TAG_CONVERSION_ERROR = "CONVERSION_ERROR";

  @Autowired
  public ConvertorService(ConvertorConfig config, Downloader downloader, Uploader uploader, Tagger tagger,
                          PcapConvertor pcapConvertor, FileHelper fileHelper, AthenaTools athena) {
    this.downloader = downloader;
    this.uploader = uploader;
    this.tagger = tagger;
    this.pcapConvertor = pcapConvertor;
    this.fileHelper = fileHelper;
    this.config = config;
    this.athena = athena;
  }

  @PostConstruct
  public void logConfig() {
    logger.info("**** settings of ConvertorService ****");
    config.logConfig();
    logger.info("**************************************");
  }

  private void tagFailed(S3PcapFile pcapFile, Exception ex) {
    Map<String, String> tags = tagger.getTags(pcapFile);
    logger.debug("Setting CONVERSION_STATUS = FAILED for {}", pcapFile);
    tags.put(TAG_CONVERSION_STATUS, ConversionStatus.FAILED.name());
    if (ex != null && ex.getMessage() != null) {
      tags.put(TAG_CONVERSION_ERROR, ex.getMessage());
    }
    tagger.addTags(pcapFile, tags);
  }

  private boolean canWeProcess(String status) {
    if (status == null) {
      return true;
    }
    return !ConversionStatus.DONE.is(status) && !ConversionStatus.FAILED.is(status);
  }

  private boolean tagAsBusy(S3PcapFile pcapFile) {
    Map<String, String> tags = tagger.getTags(pcapFile);
    String status = tags.get(TAG_CONVERSION_STATUS);
    if (canWeProcess(status))  {
      logger.info("PCAP file {} : {} = {} => let's process it now.", pcapFile.getKey(), TAG_CONVERSION_STATUS, status);
      logger.info("Tagging as BUSY : {}", pcapFile);
      tags.put(TAG_CONVERSION_STATUS, ConversionStatus.BUSY.name());
      tags.put(TAG_CONVERSION_STARTED, Instant.now().toString());
      tagger.addTags(pcapFile, tags);
      return true;
    }
    logger.info("PCAP file {} has {} = {} => skipping it", pcapFile.getKey(), TAG_CONVERSION_STATUS, status);
    return false;
  }

  public void tagDone(S3PcapFile pcapFile) {
    Map<String, String> tags = tagger.getTags(pcapFile);
    logger.info("Tagging as DONE : {}", pcapFile);
    tags.put(TAG_CONVERSION_STATUS, ConversionStatus.DONE.name());
    tags.put(TAG_CONVERSION_ENDED, Instant.now().toString());
    tagger.addTags(pcapFile, tags);
  }

  public boolean movePcapFile(S3PcapFile pcapFile, ServerInfo server) {
    if (pcapFile.isSkipped()) {
      logger.info("pcapFile {} marked skipped => not moving", pcapFile.getFileName());
      return true;
    }
    logger.info("moving PCAP file of {} to archive bucket {}", server, config.getArchiveBucketName());
    String newKey = pcapFile.improvedKey(config.getArchivePrefix(), server);
    return downloader.move(pcapFile.getObjectSummary().getBucketName(), pcapFile.getKey(), config.getArchiveBucketName(), newKey);
  }

  public ConvertorConfig getConfig() {
    return config;
  }

  public void listFiles(LocalDate day) {
    logger.info("listFiles for {}", day);
    List<S3PcapFile> allFiles = new ArrayList<>();
    long totalTODO = 0;
    long totalDONE = 0;
    long totalBusy = 0;
    for (String serverName : config.getServerNames()) {
      long bytes = 0;
      long bytesTODO = 0;
      long bytesDONE = 0;
      long bytesBusy = 0;
      logger.info(" => listFiles for serverName = {} and day={}", serverName, day);
      String prefix = serverName + config.getServerSuffix() + "/" + day.format(DateTimeFormatter.ofPattern("yyyy'_'MM'_'dd"));
      logger.info("prefix = {}", prefix);
      List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
      for (S3PcapFile file : files) {
        bytes += file.getObjectSummary().getSize();
        Map<String, String> tags = tagger.getTags(file);
        String status = tags.get(TAG_CONVERSION_STATUS);
        if (status == null) {
          bytesTODO += file.getObjectSummary().getSize();
        }
        if (ConversionStatus.DONE.name().equals(status)) {
          bytesDONE += file.getObjectSummary().getSize();
        }
        if (ConversionStatus.BUSY.name().equals(status)) {
          bytesBusy += file.getObjectSummary().getSize();
        }
      }
      logger.error("server {} => {} files => {}", serverName, files.size(), FileSize.friendlySize(bytes));
      logger.error("server {} => DONE => {}", serverName, FileSize.friendlySize(bytesDONE));
      logger.error("server {} => TODO => {}", serverName, FileSize.friendlySize(bytesTODO));
      logger.error("server {} => BUSY => {}", serverName, FileSize.friendlySize(bytesBusy));
      totalTODO += bytesTODO;
      totalDONE += bytesDONE;
      totalBusy += bytesBusy;
      allFiles.addAll(files);
    }
    logger.info("Day {} => found {} files from {} servers", day, allFiles.size(), config.getServerNames().size());
    logger.info("Day {} => TODO: {}", day, FileSize.friendlySize(totalTODO));
    logger.info("Day {} => DONE: {}", day, FileSize.friendlySize(totalDONE));
    logger.info("Day {} => BUSY: {}", day, FileSize.friendlySize(totalBusy));
    long totalSize = allFiles.stream().mapToLong(S3PcapFile::size).sum();
    logger.info("day {} => totalSize = {}", day, FileSize.friendlySize(totalSize));
  }

  /**
   * retrieve the PCAP files for given job
   * @param job the conversion job to process
   */
  void findPcapFiles(ConversionJob job) {
    logger.info("findPcapFiles: job = {}", job);
    String prefix = job.getPcapPrefix(config.isPcapFoldersNewStyle());
    logger.info("prefix = {}", prefix);
    List<S3PcapFile> files = downloader.listFilesIn(config.getPcapBucketName(), prefix);
    logger.info("we found {} pcap files for {}", files.size(), job);
    job.setPcapFiles(files);
  }

  void downloadPcapFiles(ConversionJob job) throws IOException {
    logger.info("downloadPcapFiles: job = {}", job);
    File downloadFolder = new File(config.getPcapDownloadFolder());
    int filesDone = 0;
    int filesTotal = job.getPcapFiles().size();
    long bytesDownloaded = 0;
    long bytesTotal = job.getPcapFiles().stream().mapToLong(S3PcapFile::size).sum();
    for (S3PcapFile pcapFile : job.getPcapFiles()) {
      pcapFile.setDownloadFolder(downloadFolder);
      if (pcapFile.isFullyDownloaded()) {
        logger.debug("PCAP {} was already downloaded", pcapFile.getFileName());
        filesDone++;
      } else {
        if (tagAsBusy(pcapFile)) {
          downloader.download(pcapFile.getObjectSummary(), pcapFile.getLocalFile());
          filesDone++;
          bytesDownloaded += pcapFile.getLocalFile().length();
        } else {
          logger.warn("marking {} as skipped: file will stay on S3 and tag should be reset manually", pcapFile);
          pcapFile.setSkipped(true);
        }
      }
      logger.info("Downloaded {} of {} PCAP files: {} of {}",
          filesDone, filesTotal, FileSize.friendlySize(bytesDownloaded), FileSize.friendlySize(bytesTotal));
    }
    job.markPcapsDownloaded();
  }

  public void convertPcapFiles(ConversionJob job) throws InterruptedException {
    pcapConvertor.convertToParquet(job.getLocalConversionJob());
    job.markPcapFilesConverted();
  }

  public void uploadParquetFiles(ConversionJob job) {
    int filesUploaded = 0;
    long bytesUploaded = 0;
    long bytesTotal = job.getParquetFiles().stream().mapToLong(ParquetFile::size).sum();
    int filesTotal = job.getParquetFiles().size();

    for (ParquetFile parquetFile : job.getParquetFiles()) {
      String key = parquetFile.getKey();
      if (parquetFile.matches(job.getServer())) {
        logger.info("OK, uploading file because key matches server");
        String fullKey = config.getParquetPrefix() + key;
        logger.info("Uploading s3://{}/{}", config.getParquetBucketName(), fullKey);
        if (uploader.upload(config.getParquetBucketName(), fullKey, parquetFile)) {
          filesUploaded++;
          bytesUploaded += parquetFile.size();
          logger.debug("upload done: {}", fullKey);
        }
        logger.info("Uploaded {} of {} parquet files: {} of {}",
            filesUploaded, filesTotal, FileSize.friendlySize(bytesUploaded), FileSize.friendlySize(bytesTotal));
      }
    }

    logger.info("uploaded {} parquet files", filesUploaded);
    job.setUploadCount(filesUploaded);
  }

  public void createAthenaPartitions(ConversionJob job) {
    logger.info("createAthenaPartitions for server={} and days = {}", job.getServer(), job.getDays());
    for (LocalDate day : job.getDays()) {
      logger.info("create Athena partition for {} and {}", day, job.getServer());
      addPartition(day, job.getServer());
    }
    job.markAthenaPartitionsCreated();
  }

  private void addPartition(LocalDate day, ServerInfo server) {
    athena.addPartition(day, server, config.getAthenaDatabaseName(), config.getAthenaTableName(), config.getParquetS3Location());
  }

  public void deleteLocalFiles(ConversionJob job) {
    logger.info("deleteLocalFiles for {}", job);
    logger.info("deleteLocalFiles: {} pcapFiles", job.getPcapFiles().size());
    for (S3PcapFile pcapFile : job.getPcapFiles()) {
      logger.info("deleteLocalFiles: deleting pcap file: {}", pcapFile.getFileName());
      fileHelper.delete(pcapFile.getLocalFile());
    }
    for (ParquetFile parquetFile : job.getParquetFiles()) {
      logger.info("deleteLocalFiles: deleting parquet folder: {}", parquetFile.getBaseFolder());
      fileHelper.deleteRecursively(parquetFile.getBaseFolder());
    }
    job.markLocalFilesDeleted();
  }

  public void movePcapFilesToArchiveBucket(ConversionJob job) {
    boolean ok = true;
    for (S3PcapFile pcapFile : job.getPcapFiles()) {
      ok = ok && movePcapFile(pcapFile, job.getServer());
    }
    if (ok) {
      job.markPcapFilesArchived();
    } else {
      job.markFailed("Failed to move all PCAP files to archive bucket");
    }
  }

  public void tagPcapFilesDone(ConversionJob job) {
    for (S3PcapFile pcapFile : job.getPcapFiles()) {
      tagDone(pcapFile);
    }
  }

  public void execute(ConversionJob job) {
    boolean removeTags = false;
    try {
      findPcapFiles(job);
      if (job.getPcapFiles().isEmpty()) {
        logger.info("No PCAP files found => job done: {}", job);
        logger.info("Job finished: {}", job.summary());
        return;
      }

      checkIfEnoughDiskSpace(job);

      removeTags = true;
      downloadPcapFiles(job);
      convertPcapFiles(job);
      uploadParquetFiles(job);
      createAthenaPartitions(job);
      removeTags = false;
      deleteLocalFiles(job);
      tagPcapFilesDone(job);
      movePcapFilesToArchiveBucket(job);
      logger.info("Job finished: {}", job.summary());
    } catch (Exception e) {
      job.markFailed(e.getMessage());
      logger.info("Job failed: {}", job.summary());
      logger.error("Job failed: ", e);
      if (removeTags) {
        removeTags(job);
      }
    }
  }

  private void checkIfEnoughDiskSpace(ConversionJob job) throws Exception {
    long totalBytes = job.getPcapFiles().stream().mapToLong(f -> f.getObjectSummary().getSize()).sum();
    long freeBytes = fileHelper.getFreeDiskSpaceInBytes(config.getPcapDownloadFolder());
    if (freeBytes < totalBytes) {
      logger.error("We need at least {} of free disk space but we only have {} in {}",
          FileSize.friendlySize(totalBytes), FileSize.friendlySize(freeBytes),
          config.getPcapDownloadFolder());
      throw new Exception("Not enough disk space");
    }
  }

  private void removeTags(ConversionJob job) {
    for (S3PcapFile pcapFile : job.getPcapFiles()) {
      tagger.removeTag(config.getPcapBucketName(), pcapFile.getKey(), TAG_CONVERSION_STATUS);
    }
  }

}





