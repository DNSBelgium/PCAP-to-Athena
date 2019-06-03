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

import be.dnsbelgium.data.pcap.utils.FileSize;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class Downloader {

  private final AmazonS3 amazonS3;

  private static final Logger logger = LoggerFactory.getLogger(Downloader.class);


  private final static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  public Downloader(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
  }

  public static boolean is(S3ObjectSummary summary, StorageClass storageClass) {
    String storage = storageClass.toString();
    return storage.equals(summary.getStorageClass());
  }

  /**
   * Retrieves all files with storageClass Glacier in given bucket and matching given prefix
   * @param bucketName name of bucket to search
   * @param prefix prefix to match
   * @return list of S3ObjectSummary objects of files with storageClass Glacier
   */
  public List<S3ObjectSummary> findGlacierFilesIn(String bucketName, String prefix) {
    // TODO: not a good idea to add all objects in a list => will use lots of memory (we already have almost one million PCAP files)

    logger.info("Retrieving list of files in {} / {}", bucketName, Strings.nullToEmpty(prefix));

    ObjectListing objectListing = amazonS3.listObjects(bucketName, prefix);
    List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
    logger.info("Found {} objects in {}/{}", summaries.size(), bucketName, Strings.nullToEmpty(prefix));

    List<S3ObjectSummary> result = new ArrayList<>();

    while (true) {
      for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {

        logger.info("{} => storage: {}", summary.getKey(), summary.getStorageClass());

        if (is(summary, StorageClass.Glacier)) {
          result.add(summary);
        }
      }
      if (objectListing.isTruncated()) {
        logger.debug("Get next batch of S3 objects in {}/{}", bucketName, Strings.nullToEmpty(prefix));
        objectListing = amazonS3.listNextBatchOfObjects(objectListing);
      } else {
        logger.debug("Processed all S3 objects in {}/{}", bucketName, Strings.nullToEmpty(prefix));
        break;
      }
    }
    return result;
  }

  public int requestRestoreFromGlacierByPrefix(String bucketName, String prefix, int expirationInDays) {
    logger.info("Retrieving list of files in {} / {}", bucketName, Strings.nullToEmpty(prefix));

    ObjectListing objectListing = amazonS3.listObjects(bucketName, prefix);
    List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
    logger.info("Found {} objects in {}/{}", summaries.size(), bucketName, Strings.nullToEmpty(prefix));

    int restoreRequests = 0;

    while (true) {
      for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {

        if (is(summary, StorageClass.Glacier)) {
          requestRestoreFromGlacier(bucketName, summary.getKey(), expirationInDays);
          restoreRequests++;
        }
      }
      if (objectListing.isTruncated()) {
        logger.debug("Get next batch of S3 objects in {}/{}", bucketName, Strings.nullToEmpty(prefix));
        objectListing = amazonS3.listNextBatchOfObjects(objectListing);
      } else {
        logger.debug("Processed all S3 objects in {}/{}", bucketName, Strings.nullToEmpty(prefix));
        break;
      }
    }
    return restoreRequests;
  }

  public void requestRestoreFromGlacier(String bucketName, String key, int expirationInDays) {
    logger.info("Requesting restoration from Glacier of bucket {} and key {} with expirationInDays={}",
        bucketName, key, expirationInDays);
    RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, expirationInDays);
    requestRestore.setGlacierJobParameters(new GlacierJobParameters().withTier(Tier.Bulk));

    amazonS3.restoreObjectV2(requestRestore);
    // Check the restoration status of the object.
    ObjectMetadata response = amazonS3.getObjectMetadata(bucketName, key);
    Boolean restoreFlag = response.getOngoingRestore();
    if (restoreFlag) {
      logger.info("Restoration in progress: {}", key);
    } else {
      logger.warn("Restoration of {} not in progress (already finished or failed)");
    }
  }

  public List<S3PcapFile> listFilesIn(String bucketName, String prefix) {
    logger.info("Retrieving list of files in {}/{}", bucketName, Strings.nullToEmpty(prefix));
    // S3 docs: List results are <i>always</i> returned in lexicographic (alphabetical) order.
    // This is not ideal since we want to process the files in chronological order
    // and they are now stored in bucket/server/dd_mm_yyyy/ folders ...
    // see below: S3PcapFile.compare will sort by server and date

    ObjectListing objectListing = amazonS3.listObjects(bucketName, prefix);
    List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
    logger.info("Found {} objects in {}/{}", summaries.size(), bucketName, Strings.nullToEmpty(prefix));

    List<S3PcapFile> files = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {

      logger.debug("found [{}]", summary.getKey());
      boolean include = true;

      if (summary.getKey().endsWith("/")) {
        logger.info("{} is a folder => skip", summary.getKey());
        include = false;
      }
      if (is(summary, StorageClass.Glacier)) {
        logger.warn("File {} is on Glacier => skip", summary.getKey());
        include = false;
      }
      if (include) {
        S3PcapFile file = S3PcapFile.parse(summary);
        if (file != null) {
          files.add(file);
        }
      }
    }
    // we need to sort since currently S3 keys are not in chronological order
    // see be.dnsbelgium.data.pcap.aws.s3.S3PcapFile.compareTo
    Collections.sort(files);
    return files;
  }

  /**
   * Download a S3 Object and save it to the given path
   * @param objectSummary the S3ObjectSummary
   * @param localFile the downloaded path
   * @return localFile
   * @throws IOException if an I/O error occurs
   */
  public File download(S3ObjectSummary objectSummary, File localFile) throws IOException {
    File folder = localFile.getParentFile();

    logger.info("Downloading file from S3 {} in {} ({})", localFile.getName(), folder, FileSize.friendlySize(objectSummary.getSize()));

    long startDownload = System.currentTimeMillis();
    S3Object s3object = amazonS3.getObject(objectSummary.getBucketName(), objectSummary.getKey());

    try (InputStream inputStream = s3object.getObjectContent()) {
      Files.copy(inputStream, localFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      logger.info("** file saved as " + localFile.getAbsolutePath());
    }
    long millis = System.currentTimeMillis() - startDownload;
    logger.info("** download took {}ms => {}", millis, FileSize.friendlyThroughput(objectSummary.getSize(), millis));
    return localFile;
  }

  private void copyObject(String sourceBucket, String sourceKey, String destBucket, String destKey) {
    logger.info("Copying from bucket [{}] to [{}] \noldKey:{} \nnewKey:{}", sourceBucket, destBucket, sourceKey, destKey);
    amazonS3.copyObject(sourceBucket, sourceKey, destBucket, destKey);
  }

  public boolean copy(String sourceBucket, String sourceKey, String destBucket, String destKey) {
    try {
      copyObject(sourceBucket, sourceKey, destBucket, destKey);
      return true;
    } catch (AmazonServiceException e) {
      logger.error("Failed to copy {} from to {}. {} : {}", sourceKey, sourceBucket, destBucket, e.getClass().getSimpleName(), e.getMessage());
      return false;
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  public boolean move(String sourceBucket, String sourceKey, String destBucket, String destKey) {
    try {
      copyObject(sourceBucket, sourceKey, destBucket, destKey);
      logger.info("Copy done. Now deleting {} in {}", sourceKey, sourceBucket);
      amazonS3.deleteObject(sourceBucket, sourceKey);
      return true;
    } catch (AmazonServiceException e) {
      logger.error("Failed to move {} from to {}. {} : {}", sourceKey, sourceBucket, destBucket, e.getClass().getSimpleName(), e.getMessage());
      return false;
    }
  }

  static String describe(S3ObjectSummary objectSummary) {
    return String.format(
        "bucket: %s key: %s : size: %s  last-modified: %s  storage-class: %s",
        objectSummary.getBucketName(),
        objectSummary.getKey(),
        FileSize.friendlySize(objectSummary.getSize()),
        df.format(objectSummary.getLastModified()),
        objectSummary.getStorageClass()
    );
  }

  @SuppressWarnings("unused")
  public int restoreFromGlacier(String bucketName, String prefix, int expirationInDays) {

    logger.info("Requesting restoration from Glacier of bucket {} and prefix {} with expirationInDays={}", bucketName, prefix, expirationInDays);
    List<S3ObjectSummary> summaries = findGlacierFilesIn(bucketName, prefix);

    logger.info("Restoring {} files", summaries.size());

    int restoresRequested = 0;

    for (S3ObjectSummary summary : summaries) {

      RestoreObjectRequest requestRestore = new RestoreObjectRequest(summary.getBucketName(), summary.getKey(), expirationInDays);
      amazonS3.restoreObjectV2(requestRestore);

      // Check the restoration status of the object.
      ObjectMetadata response = amazonS3.getObjectMetadata(bucketName, summary.getKey());
      Boolean restoreFlag = response.getOngoingRestore();

      restoresRequested++;

      if (restoreFlag) {
        logger.debug("Restoration of {} in progress", summary.getKey());
      } else {
        logger.warn("Restoration of {} not in progress (already finished or failed)");
      }
    }
    return restoresRequested;

  }

  public List<String> listFolders(String bucketName, String prefix) {
    logger.info("listFolders in in bucket {} with prefix {}", bucketName, prefix);
    ObjectListing listing = amazonS3.listObjects(new ListObjectsRequest(bucketName, prefix, null, "/", null));
    return new ArrayList<>(listing.getCommonPrefixes());
  }


}
