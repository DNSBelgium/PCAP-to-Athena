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
