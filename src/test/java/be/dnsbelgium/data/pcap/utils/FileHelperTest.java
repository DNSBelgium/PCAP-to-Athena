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

import org.junit.Before;
import org.junit.Ignore;
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
  @Ignore
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
