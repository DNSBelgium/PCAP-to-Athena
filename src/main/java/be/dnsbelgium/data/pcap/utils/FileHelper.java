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

package be.dnsbelgium.data.pcap.utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class FileHelper {

  private static final Logger logger = getLogger(FileHelper.class);

  public List<File> findRecursively(File folder, String extension) {
    return new ArrayList<>(FileUtils.listFiles(folder, new String[]{extension}, true));
  }

  public void delete(File file) {
    boolean deleted = file.delete();
    if (deleted) {
      logger.info("Local file deleted: {}", file);
    } else {
      logger.error("Failed to delete local file {}. Trying to delete on exit.", file);
      file.deleteOnExit();
    }
  }

  public void deleteRecursively(File directory) {
    try {
      FileUtils.deleteDirectory(directory);
    } catch (IOException e) {
      logger.warn("Failed to delete directory " + directory, e);
    }
  }

  /**
   * Create a File object representing a randomly named subfolder within given folder.
   * It does not create this subfolder.
   * Moved to separate class for easier mocking and testing.
   *
   * @return a File object representing a randomly named subfolder within given folder.
   */
  public File uniqueSubFolder(String folder) {
    String uuid = UUID.randomUUID().toString().replaceAll("-", "_");
    File subFolder = new File(folder, uuid);
    logger.info("uniqueSubFolder: {}", subFolder);
    return subFolder;
  }

  public long getFreeDiskSpaceInBytes(String path) {
    long free = new File(path).getUsableSpace();
    logger.info("We have {} disk space available in {}", FileSize.friendlySize(free), path);
    return free;
  }

}
