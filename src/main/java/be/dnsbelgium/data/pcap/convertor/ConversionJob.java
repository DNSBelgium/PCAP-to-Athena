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

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.aws.s3.ParquetFile;
import be.dnsbelgium.data.pcap.aws.s3.S3PcapFile;
import be.dnsbelgium.data.pcap.utils.FileSize;
import org.slf4j.Logger;

import java.io.File;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class ConversionJob {

  public enum Status {
    INITIAL,
    PCAP_FILES_LISTED,
    PCAP_FILES_DOWNLOADED,
    PCAP_FILES_CONVERTED,
    PARQUET_FILES_UPLOADED,
    ATHENA_PARTITIONS_CREATED,
    LOCAL_FILES_DELETED,
    PCAP_FILES_ARCHIVED,
    FAILED
  }

  private static final Logger logger = getLogger(ConversionJob.class);

  private Status status = Status.INITIAL;
  private final ServerInfo server;
  private final LocalDate date;
  private List<S3PcapFile> pcapFiles = new ArrayList<>();
  private File parquetOutputFolder;

  private LocalDateTime startTime;
  private LocalDateTime finishTime;
  private int uploadCount;
  private String errorMessage;
  private LocalConversionJob localConversionJob;

  public ConversionJob(ServerInfo server, LocalDate date, File parquetOutputFolder) {
    this.server = server;
    this.date = date;
    this.parquetOutputFolder = parquetOutputFolder;
    this.startTime = LocalDateTime.now();
  }

  public LocalConversionJob getLocalConversionJob() {
    if (localConversionJob == null) {
      localConversionJob = new LocalConversionJob(server, getLocalPcapFiles(), parquetOutputFolder);
    }
    return localConversionJob;
  }

  public void markFailed(String message) {
    status = Status.FAILED;
    this.errorMessage = message;
    logger.error("Job failed: {}", this);
  }


  public List<File> getLocalPcapFiles() {
    return pcapFiles.stream()
        .filter(S3PcapFile::isDownloaded)
        .map(S3PcapFile::getLocalFile).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConversionJob.class.getSimpleName() + "[", "]")
        .add("status=" + status)
        .add("server=" + server)
        .add("start=" + startTime)
        .add("finish=" + finishTime)
        .add("date=" + date)
        .add("pcapFiles=" + pcapFiles.size())
        .add("localJob=" + localConversionJob)
        .add("errorMessage=" + errorMessage)
        .toString();
  }

  public void logStatus() {
    logger.info("server={} dmy={} => status={}", server.getName(), date, status);
  }

  /**
   * return a list of all days for which this job has created Parquet files
   *
   * @return List of LocalDate
   */
  public List<LocalDate> getDays() {
    return (localConversionJob == null) ? Collections.emptyList() : localConversionJob.getDaysCovered();
  }

  public Status getStatus() {
    return status;
  }

  public ServerInfo getServer() {
    return server;
  }

  public List<S3PcapFile> getPcapFiles() {
    return pcapFiles;
  }

  public List<ParquetFile> getParquetFiles() {
    return (localConversionJob == null) ? Collections.emptyList() : localConversionJob.getParquetFiles();
  }

  public void setPcapFiles(List<S3PcapFile> pcapFiles) {
    if (status != Status.INITIAL) {
      throw new IllegalStateException("Cannot only set PCAP files when status = INITIAL but is " + status);
    }
    this.pcapFiles = pcapFiles;
    this.status = Status.PCAP_FILES_LISTED;
    logStatus();
  }

  public void markPcapsDownloaded() {
    if (status != Status.PCAP_FILES_LISTED) {
      throw new IllegalStateException("Can only set PCAP files as downloaded when status = PCAP_FILES_LISTED but is " + status);
    }
    if (allPcapsDownloaded()) {
      status = Status.PCAP_FILES_DOWNLOADED;
    } else {
      throw new IllegalStateException("Not all PCAP files downloaded. Download failed ?");
    }
    logStatus();
  }

  public void markPcapFilesConverted() {
    if (status != Status.PCAP_FILES_DOWNLOADED) {
      throw new IllegalStateException("Can only set to PCAP_FILES_CONVERTED when status = PCAP_FILES_DOWNLOADED but is " + status);
    }
    status = Status.PCAP_FILES_CONVERTED;
    logStatus();
  }

  public void markAthenaPartitionsCreated() {
    if (status != Status.PARQUET_FILES_UPLOADED) {
      throw new IllegalArgumentException("Can only set to ATHENA_PARTITIONS_CREATED when status = PARQUET_FILES_UPLOADED but is " + status);
    }
    status = Status.ATHENA_PARTITIONS_CREATED;
    logStatus();
  }

  public boolean allPcapsDownloaded() {
    for (S3PcapFile pcapFile : pcapFiles) {
      if (!pcapFile.isDownloaded() && !pcapFile.isSkipped()) {
        logger.warn("Not correctly downloaded: {}", pcapFile.determineLocalFileName());
        return false;
      }
    }
    return true;
  }

  public String getPcapPrefix(boolean newStyle) {
    if (newStyle) {
      String pattern = "server=%s/year=%04d/month=%02d/day=%02d/";
      return String.format(pattern, server.getFullname(), date.getYear(), date.getMonthValue(), date.getDayOfMonth());
    } else {
      return server.getFullname() + "/" + date.format(DateTimeFormatter.ofPattern("dd'-'MM'-'yyyy")) + "/";
    }
  }


  public void markLocalFilesDeleted() {
    if (status != Status.ATHENA_PARTITIONS_CREATED) {
      throw new IllegalArgumentException("Can only set to LOCAL_FILES_DELETED when status = ATHENA_PARTITIONS_CREATED but is " + status);
    }
    status = Status.LOCAL_FILES_DELETED;
    logStatus();
  }

  public void markPcapFilesArchived() {
    if (status != Status.LOCAL_FILES_DELETED) {
      throw new IllegalArgumentException("Can only set to PCAP_FILES_ARCHIVED when status = LOCAL_FILES_DELETED but is " + status);
    }
    status = Status.PCAP_FILES_ARCHIVED;
    finishTime = LocalDateTime.now();
    logStatus();
  }

  public void setUploadCount(int uploadCount) {
    if (status != Status.PCAP_FILES_CONVERTED) {
      throw new IllegalArgumentException("Can only setUploadCount when status = PCAP_FILES_CONVERTED but is " + status);
    }
    logger.info("uploaded {} parquet files", uploadCount);
    this.uploadCount = uploadCount;
    status = Status.PARQUET_FILES_UPLOADED;
    logStatus();
  }

  public int getUploadCount() {
    return uploadCount;
  }

  public String summary() {
    StringBuilder builder = new StringBuilder();
    String pcapBytes    = FileSize.friendlySize(getLocalConversionJob().getTotalPcapBytes());
    String parquetBytes = FileSize.friendlySize(getLocalConversionJob().getTotalParquetBytes());
    LocalDateTime end = (finishTime == null) ? LocalDateTime.now() : finishTime;

    String duration = Duration.between(startTime, end).toString();

    builder.append("\n")
        .append(" ==== Job: =======================================").append("\n")
        .append("  Day            : ").append(date.format(DateTimeFormatter.ISO_LOCAL_DATE)).append("\n")
        .append("  Server         : ").append(server.getFullname()).append("\n")
        .append("  Started        : ").append(startTime).append("\n")
        .append("  Finished       : ").append(finishTime).append("\n")
        .append("  Duration       : ").append(duration).append("\n")
        .append("  Status         : ").append(status).append("\n")
        .append("  PCAP files     : ").append(pcapFiles.size()).append("\n")
        .append("  PCAP size      : ").append(pcapBytes).append("\n")
        .append("  Parquet files  : ").append(getParquetFiles().size()).append("\n")
        .append("  Parquet size   : ").append(parquetBytes).append("\n");
    if (errorMessage != null) {
      builder.append("  Error message  : ").append(errorMessage).append("\n");
    }
    builder.append("==================================================");
    return builder.toString();
  }

  public LocalDateTime getFinishTime() {
    return finishTime;
  }
}
