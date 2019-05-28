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

package be.dnsbelgium.data.pcap.aws.s3;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileSize;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class BulkDownloader {

  private final AmazonS3 amazonS3;

  private static final Logger logger = LoggerFactory.getLogger(Downloader.class);

  @Value("${pcap.bucket.name}")
  private String bucketName;

  @Value("${pcap.download.folder}")
  private String downloadFolder;

  @Value("${pcap.download.maxFiles}")
  private int maxDownloads;

  private int totalDownloads = 0;

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  public BulkDownloader(AmazonS3 amazonS3) {

    this.amazonS3 = amazonS3;
  }
  /**
   * No longer used, now that we will process files one by one (instead of per server per day)
   * @param server the details for which we want to download files
   * @param date the day for which we want to download files
   * @return the number of files downloaded
   */
  @SuppressWarnings("unused")
  public int downloadFilesFor(ServerInfo server, LocalDate date) {
    logger.info("Downloading files of server {} on {}", server, date);
    long start = System.currentTimeMillis();
    String prefix = prefix(server, date);
    ObjectListing objectListing = amazonS3.listObjects(bucketName, prefix);
    List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
    logger.info("Found {} files in {}/{}", summaries.size(), bucketName, prefix);
    int successfulDownloads = 0;
    int failedDownloads = 0;
    long totalBytes = 0;
    for (S3ObjectSummary summary : summaries) {
      String fileInfo = Downloader.describe(summary);
      try {
        if (maxDownloads > 0 && totalDownloads >= maxDownloads) {
          logger.info("totalDownloads={} >= maxDownloads={} => skipping download of {}", totalDownloads, maxDownloads, fileInfo);
        } else {
          download(server, date, summary);
          successfulDownloads++;
          totalBytes += summary.getSize();
          this.totalDownloads++;
        }
      } catch (IOException e) {
        logger.error("Failed to download " + fileInfo + " => skipping this file.", e);
        failedDownloads++;
      }
    }
    long millis = System.currentTimeMillis() - start;
    logger.info("Downloaded {} files ({}) in {} ms => {}",
        successfulDownloads,
        FileSize.friendlySize(totalBytes),
        millis,
        FileSize.friendlyThroughput(totalBytes, millis));
    logger.info("Finished downloading for server {} and day {} => Success: {}  Failed: {}",
        server, date, successfulDownloads, failedDownloads);
    return successfulDownloads;
  }


  private void download(ServerInfo server, LocalDate date, S3ObjectSummary objectSummary) throws IOException {
    String fileInfo = Downloader.describe(objectSummary);
    logger.info("downloading {}", fileInfo);

    long startDownload = System.currentTimeMillis();
    S3Object s3object = amazonS3.getObject(bucketName, objectSummary.getKey());
    File file = new File(objectSummary.getKey());
    String fileName = date.format(DateTimeFormatter.ofPattern("yyyy'_'MM'_'dd")) + "_" + file.getName();
    if (!fileName.endsWith(".pcap.gz")) {
      logger.debug("appending .pcap.gz since the parquet converter expects it");
      fileName += ".pcap.gz";
    }
    String folderName = downloadFolder + "/" + server.getFullname() + "/";
    logger.info("saving file in {}", folderName);
    File target = new File(folderName + fileName);
    File folder = new File(folderName);

    if (!folder.exists()) {
      logger.info("creating folder {}", folder);
      Files.createDirectories(folder.toPath());
    }

    try (InputStream inputStream = s3object.getObjectContent()) {
      Files.copy(inputStream, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      logger.info("** file saved as " + target.getAbsolutePath());
      long millis = System.currentTimeMillis() - startDownload;
      logger.info("** download took {}ms => {}", millis, FileSize.friendlyThroughput(objectSummary.getSize(), millis));
    }
  }

  private String prefix(ServerInfo server, LocalDate date) {
    return server.getFullname() + "/" + date.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/";
  }

}
