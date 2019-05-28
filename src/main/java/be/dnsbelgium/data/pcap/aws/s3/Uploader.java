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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;

@Component
public class Uploader {

  private final AmazonS3 amazonS3;

  private static final Logger logger = LoggerFactory.getLogger(Uploader.class);

  @Value("${parquet.delete.after.upload}")
  private boolean deleteAfterUpload;

  @Autowired
  public Uploader(@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
    logger.info("amazonS3.getRegionName() = {}", amazonS3.getRegionName());
    logger.info("Current AWS account: {}",  amazonS3.getS3AccountOwner().getDisplayName());
  }

  @PostConstruct
  public void logConfig() {
    logger.info("*** config used by Uploader ***");
    logger.info("**************************************");
  }

  public boolean upload(String bucketName, String key, ParquetFile parquetFile) {
    return upload(bucketName, key, parquetFile.getFile());
  }

  public boolean upload(String bucketName, String key, File file) {
    logger.info("  uploading file: {}", file);
    logger.info("  with key : {}", key);
    long start = System.currentTimeMillis();
    logger.info("  uploading a file of {}", FileSize.friendlySize(file.length()));
    try {
      amazonS3.putObject(bucketName, key, file);
      logger.info("  uploaded: {} bytes", FileSize.friendlySize(file.length()));
      long millis = System.currentTimeMillis() - start;
      logger.info("  upload took {}ms => {}", millis, FileSize.friendlyThroughput(file.length(), millis));
      deleteLocally(file);
      return true;
    } catch (AmazonServiceException e) {
      logger.error("Upload failed", e);
      return false;
    }
  }

  private void deleteLocally(File file) {
    if (deleteAfterUpload) {
      boolean deleted = file.delete();
      logger.info("deleted {} => {}", file, deleted);
    }
  }





}