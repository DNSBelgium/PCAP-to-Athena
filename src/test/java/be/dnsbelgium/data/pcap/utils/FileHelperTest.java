package be.dnsbelgium.data.pcap.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

public class FileHelperTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Logger logger = getLogger(FileHelperTest.class);
  private FileHelper fileHelper = new FileHelper();
  private File tempFolder;

  @Before
  public void setUp() throws IOException {
    tempFolder = temporaryFolder.newFolder();
  }

  @Test
  public void testFreeDiskSpaceInBytes() {
    long free = fileHelper.getFreeDiskSpaceInBytes(tempFolder.getPath());
    logger.info("free = {}", free);
    assertEquals(free, tempFolder.getUsableSpace());
  }

  @Test
  public void testUniqueSubFolder() throws IOException {
    File folder = fileHelper.uniqueSubFolder(tempFolder.getPath());
    assertThat(folder.exists(), is(false));
    assertThat(folder.mkdir(), is(true));
    File temp = new File(folder, "test.txt");
    assertThat(temp.createNewFile(), is(true));
    fileHelper.deleteRecursively(folder);
    assertThat(folder.exists(), is(false));
  }

  @Test
  public void deleteNonExistingFolder() {
    File folder = fileHelper.uniqueSubFolder(tempFolder.getPath());
    assertFalse("should not yet exist", folder.exists());
    fileHelper.deleteRecursively(folder);
    assertFalse("should still not exist", folder.exists());
  }

  @Test
  public void deleteRecursivelyOnFile() throws IOException {
    File file = new File(tempFolder, "abc.txt");
    logger.info("file created: {}", file.createNewFile());
    try {
      fileHelper.deleteRecursively(file);
      fail("should throw IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    } finally {
      fileHelper.delete(file);
    }
  }

  @Test
  public void testDelete() throws IOException {
    File file = new File(tempFolder , "abc.txt");
    logger.info("file created: {}", file.createNewFile());
    assertThat(file.exists(), is(true));
    fileHelper.delete(file);
    assertThat(file.exists(), is(false));
    fileHelper.delete(new File(tempFolder, "non-existing-file"));
  }

}