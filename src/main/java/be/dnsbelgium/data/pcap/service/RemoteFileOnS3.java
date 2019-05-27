package be.dnsbelgium.data.pcap.service;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.nio.file.Path;

/*
    This class represents an Object on S3 (and can be downloaded) or a local File that could be uploaded to S3
 */
public class RemoteFileOnS3 implements RemoteFile {

  private final S3ObjectSummary objectSummary;
  private Path localPath;

  public RemoteFileOnS3(S3ObjectSummary objectSummary) {
    this.objectSummary = objectSummary;
  }

  public RemoteFileOnS3(Path path, String key) {
    this.objectSummary = new S3ObjectSummary();
    objectSummary.setKey(key);
    objectSummary.setSize(path.toFile().length());
  }

  @Override
  public long size() {
    return objectSummary.getSize();
  }

  @Override
  public String getKey() {
    return objectSummary.getKey();
  }

  @Override
  public void setKey(String key) {
    objectSummary.setKey(key);
  }

  @Override
  public Path getLocalPath() {
    return localPath;
  }

  @Override
  public void setLocalPath(Path path) {
    this.localPath = path;
  }

  @Override
  public boolean existsLocally() {
    return localPath.toFile().exists();
  }
}
