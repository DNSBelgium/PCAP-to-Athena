package be.dnsbelgium.data.pcap.aws.s3;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileSize;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unused")
public class BulkUploader {

  private static final Logger logger = getLogger(BulkUploader.class);

  private Uploader uploader;

  @Value("${parquet.output.folder}")
  private String localFolder;

  public BulkUploader(Uploader uploader) {
    this.uploader = uploader;
  }

  /**
   * Uploads all locally found parquet files for given server
   * @param server the Server for which we have to find and upload files
   *
   * Unused since we now plan to process files one by one instead of per server per day
   */
  public void uploadFilesFor(ServerInfo server, String bucketName) {
    logger.info("uploading all locally found parquet files for {}", server.getFullname());
    String folderName = localFolder + "/" + server.getNormalizedServerName() + "/dnsdata/";

    logger.info("Scan for parquet files in: " + folderName);
    File folder;
    try {
      folder = FileUtils.getFile(folderName);
      logger.info("folder exists:{}  isDirectory:{}  canRead:{}", folder.exists(), folder.isDirectory(), folder.canRead());
    } catch (Exception e) {
      logger.error("Failed to read folder " + folderName, e);
      return;
    }
    if (!folder.exists() ) {
      logger.error("folder for server {} does not exist => skipping this folder. path:{}", server, folderName);
      return;
    }
    Iterator<File> iterator = FileUtils.iterateFiles(folder, new String[] {"parquet"}, true);

    List<File> files = new ArrayList<>();
    long totalBytes = 0;

    while (iterator.hasNext()) {
      File parquetFile = iterator.next();
      if (!files.contains(parquetFile)) {
        files.add(parquetFile);
        totalBytes += parquetFile.length();
      }
    }
    logger.info("Server {} => found {} parquet file(s) => {}", server, files.size(), FileSize.friendlySize(totalBytes));

    // ordering probably not important but cheap anyway
    Collections.sort(files);

    int filesUploaded = 0;
    long bytesUploaded = 0;

    for (File parquetFile : files) {
      String fullPath = parquetFile.getAbsolutePath();
      logger.info("uploading {}", fullPath);

      // we have to strip the first part of the path to get the S3 key
      String toRemove = localFolder + "/" + server.getNormalizedServerName() + "/";
      String key = fullPath.replace(toRemove, "");

      String regexp = "dnsdata/year=\\d\\d\\d\\d/month=\\d\\d/day=\\d\\d/server=" + server.getFullname() + "/.*parquet";

      boolean ok = key.matches(regexp);
      if (!ok) {
        logger.error("oops, key {} does not look like expected: {}", key, regexp);
      } else {
        logger.info("OK, key matches regexp {}", regexp);
      }
      logger.info("uploading as {}", key);
      long length = parquetFile.length();
      if (uploader.upload(bucketName, key, parquetFile)) {
        filesUploaded++;
        bytesUploaded += length;
      }
    }
    logger.info("Uploaded {} files => {}", filesUploaded, FileSize.friendlySize(bytesUploaded));
  }
}
