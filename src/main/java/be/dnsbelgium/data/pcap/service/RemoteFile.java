package be.dnsbelgium.data.pcap.service;

import java.nio.file.Path;

public interface RemoteFile {

  long size();
  String getKey();
  void setKey(String key);
  Path getLocalPath();
  void setLocalPath(Path path);
  boolean existsLocally();

}
