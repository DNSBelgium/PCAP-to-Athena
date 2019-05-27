package be.dnsbelgium.data.pcap.service;

import java.util.List;

public interface RemoteFileManager {

  List<RemoteFile> findRemoteFiles(FileFilter filter);

  void archive(RemoteFile remoteFile);

  void download(List<RemoteFile> remoteFiles);

  /* mark the remote file to avoid processing by someone else */
  void tagBusy(RemoteFile file);

  /* unmark the remote file: allow processing by someone else */
  void untagBusy(RemoteFile file);

  /* check whether the file is tagged as busy */
  boolean isBusy(RemoteFile remoteFile);

  void upload(List<RemoteFile> files);



}
