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
