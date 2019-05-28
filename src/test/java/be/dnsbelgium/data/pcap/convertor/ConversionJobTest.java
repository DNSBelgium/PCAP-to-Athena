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

package be.dnsbelgium.data.pcap.convertor;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.aws.s3.ParquetFile;
import be.dnsbelgium.data.pcap.aws.s3.S3PcapFile;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.LoggerFactory.getLogger;

public class ConversionJobTest {

  private static final Logger logger = getLogger(ConversionJobTest.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ServerInfo server = new ServerInfo("dummy.dns.be", "dummy", "brussels");
  private LocalDate day = LocalDate.of(2018, 11, 5);

  private File pcapFile1;
  private File pcapFile2;

  private S3PcapFile s3PcapFile1 = mock(S3PcapFile.class);
  private S3PcapFile s3PcapFile2 = mock(S3PcapFile.class);
  private List<S3PcapFile> s3PcapFiles = Lists.newArrayList(s3PcapFile1, s3PcapFile2);

  @Before
  public void before() throws IOException {
    pcapFile1 = temporaryFolder.newFile();
    FileWriter fileWriter = new FileWriter(pcapFile1);
    fileWriter.write("test pcap file 1");
    fileWriter.close();

    pcapFile2 = temporaryFolder.newFile();
    fileWriter = new FileWriter(pcapFile2);
    fileWriter.write("test pcap file 2");
    fileWriter.close();

    when(s3PcapFile1.getLocalFile()).thenReturn(pcapFile1);
    when(s3PcapFile2.getLocalFile()).thenReturn(pcapFile2);
  }

  @Test
  public void testInitial() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);

    assertThat(job.getStatus(), is(ConversionJob.Status.INITIAL));
    assertThat(job.getDays(), empty());
    assertThat(job.getServer(), is(server));
    assertThat(job.getLocalPcapFiles(), empty());
    assertThat(job.getPcapFiles(), is(Collections.emptyList()));
    assertThat(job.getParquetFiles(), is(Collections.emptyList()));
    assertThat(job.getUploadCount(), is(0));

    assertThat(job.getLocalConversionJob().getServerInfo(), is(server));
    assertThat(job.getLocalConversionJob().getPcapFiles(), is(Collections.emptyList()));
    assertThat(job.getLocalConversionJob().getParquetOutputFolder(), is(newFile));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testPcapFilesListed() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    assertThat(job.getStatus(), is(ConversionJob.Status.PCAP_FILES_LISTED));
    assertThat(job.getPcapFiles(), is(Lists.newArrayList(s3PcapFiles)));
    assertThat(job.getLocalPcapFiles(), is(Collections.emptyList()));
    assertThat(job.getParquetFiles(), is(Collections.emptyList()));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testPcapFilesDownloaded() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    assertThat(job.getStatus(), is(ConversionJob.Status.PCAP_FILES_DOWNLOADED));
    assertThat(job.getPcapFiles(), is(Lists.newArrayList(s3PcapFiles)));
    assertThat(job.getLocalPcapFiles(), is(Lists.newArrayList(pcapFile1, pcapFile2)));
    assertThat(job.getParquetFiles(), is(Collections.emptyList()));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testPcapFilesConverted() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    job.markPcapFilesConverted();
    assertThat(job.getStatus(), is(ConversionJob.Status.PCAP_FILES_CONVERTED));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testParquetFilesUploaded() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    job.markPcapFilesConverted();
    job.setUploadCount(2);
    assertThat(job.getStatus(), is(ConversionJob.Status.PARQUET_FILES_UPLOADED));
    assertThat(job.getUploadCount(), is(2));

    File folder = temporaryFolder.newFolder();
    File file = new File(folder, "parquet");
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.write("test");
    fileWriter.close();
    job.getLocalConversionJob().addParquetFile(new ParquetFile(folder, file));
    System.out.println(job.getParquetFiles());

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testAthenaPartitionCreated() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    job.markPcapFilesConverted();
    job.setUploadCount(2);

    job.markAthenaPartitionsCreated();

    assertThat(job.getStatus(), is(ConversionJob.Status.ATHENA_PARTITIONS_CREATED));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testLocalPcapFilesDeleted() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    job.markPcapFilesConverted();
    job.setUploadCount(2);

    job.markAthenaPartitionsCreated();
    job.markLocalFilesDeleted();

    assertThat(job.getStatus(), is(ConversionJob.Status.LOCAL_FILES_DELETED));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testPcapFilesArchived() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    when(s3PcapFile1.isDownloaded()).thenReturn(true);
    when(s3PcapFile2.isDownloaded()).thenReturn(true);
    job.markPcapsDownloaded();

    job.markPcapFilesConverted();
    job.setUploadCount(2);

    job.markAthenaPartitionsCreated();
    job.markLocalFilesDeleted();
    job.markPcapFilesArchived();

    assertThat(job.getStatus(), is(ConversionJob.Status.PCAP_FILES_ARCHIVED));
    assertNotNull(job.getFinishTime());

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testFailed() throws IOException {
    File newFile = temporaryFolder.newFile();
    ConversionJob job = new ConversionJob(server, day, newFile);
    job.setPcapFiles(s3PcapFiles);

    job.markFailed("test");
    assertThat(job.getStatus(), is(ConversionJob.Status.FAILED));

    logger.info("job.summary():\n{}", job.summary());
  }

  @Test
  public void testGetPcapPrefixOldStyle() {
    ConversionJob job = new ConversionJob(server, day, null);
    String pcapPrefix = job.getPcapPrefix(false);

    assertEquals("dummy.dns.be/05-11-2018/", pcapPrefix);
  }

  @Test
  public void testGetPcapPrefixNewStyle() {
    ConversionJob job = new ConversionJob(server, day, null);
    String pcapPrefix = job.getPcapPrefix(true);

    assertEquals("server=dummy.dns.be/year=2018/month=11/day=05/", pcapPrefix);

  }

}