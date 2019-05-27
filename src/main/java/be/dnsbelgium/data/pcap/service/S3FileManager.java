package be.dnsbelgium.data.pcap.service;

import be.dnsbelgium.data.pcap.aws.s3.Downloader;
import be.dnsbelgium.data.pcap.aws.s3.Tagger;
import be.dnsbelgium.data.pcap.aws.s3.Uploader;
import be.dnsbelgium.data.pcap.convertor.ConvertorConfig;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class S3FileManager implements RemoteFileManager {

  private Downloader downloader;
  private Uploader uploader;
  private Tagger tagger;
  private ConvertorConfig config;
  private static final Logger logger = getLogger(S3FileManager.class);

  @Autowired
  public S3FileManager(Downloader downloader, Uploader uploader, Tagger tagger, ConvertorConfig config) {
    this.downloader = downloader;
    this.uploader = uploader;
    this.tagger = tagger;
    this.config = config;
  }

  @Override
  public List<RemoteFile> findRemoteFiles(FileFilter filter) {
    downloader.listFilesIn("BUCKET", filter.getPrefix());
    return null;
  }

  @Override
  public void archive(RemoteFile remoteFile) {
    String sourceBucket = config.getPcapBucketName();
    String targetBucket = config.getArchiveBucketName();
    // change key ?
    downloader.move(sourceBucket, remoteFile.getKey(), targetBucket, remoteFile.getKey());
  }

  @Override
  public void download(List<RemoteFile> remoteFiles) {
    for (RemoteFile remoteFile : remoteFiles) {
      // TODO: check and add tags ??
      tagger.getTags("", remoteFile.getKey());
      try {
        File file = downloader.download(null, remoteFile.getLocalPath().toFile());
        remoteFile.setLocalPath(file.toPath());
      } catch (IOException e) {
        logger.error("Downloading [" + remoteFile.getKey() + "] failed", e);
      }
    }
  }

  @Override
  public void tagBusy(RemoteFile file) {

  }

  @Override
  public void untagBusy(RemoteFile file) {

  }

  @Override
  public boolean isBusy(RemoteFile remoteFile) {
    return false;
  }


  @Override
  public void upload(List<RemoteFile> files) {
    String targetBucket = config.getParquetBucketName();
    // todo add prefix
    for (RemoteFile remoteFile : files) {
      uploader.upload(targetBucket, remoteFile.getKey(), remoteFile.getLocalPath().toFile());
    }
  }
}
