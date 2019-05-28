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
