package be.dnsbelgium.data.pcap.convertor;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.aws.s3.ParquetFile;
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
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.LoggerFactory.getLogger;

public class LocalConversionJobTest {

  private static final Logger logger = getLogger(LocalConversionJobTest.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ServerInfo serverInfo;
  private File outputFolder;

  private File file1;
  private List<File> pcapFiles;

  @Before
  public void setUp() throws IOException {
    serverInfo = new ServerInfo("dummy");
    outputFolder = temporaryFolder.newFolder();

    file1 = temporaryFolder.newFile();
    FileWriter fileWriter = new FileWriter(file1);
    fileWriter.write("dummy pcap file");
    fileWriter.close();

    pcapFiles = Lists.newArrayList(file1, temporaryFolder.newFile());
  }


  @Test
  public void testConstructor() {
    LocalConversionJob job = new LocalConversionJob(serverInfo, pcapFiles, outputFolder);
    logger.info("job = {}", job);
    assertEquals(Lists.newArrayList(), job.getDaysCovered());
    assertEquals(serverInfo, job.getServerInfo());
    assertEquals(pcapFiles, job.getPcapFiles());
    assertEquals(file1.length(), job.getTotalPcapBytes());
    assertEquals(Lists.newArrayList(), job.getParquetFiles());
    assertEquals(0L, job.getTotalParquetBytes());
    assertEquals(outputFolder, job.getParquetOutputFolder());
  }

  @Test
  public void testGetDaysCovered() {
    LocalConversionJob job = new LocalConversionJob(serverInfo, pcapFiles, outputFolder);
    logger.info("job = {}", job);
    job.addParquetFile(new ParquetFile(outputFolder, new File("f1.parquet")));

    assertThat(job.getParquetFiles().size(), is(1));
    assertThat(job.getDaysCovered().isEmpty(), is(true));
    assertThat(job.getTotalParquetBytes(), is(0L));

    job.addParquetFile(new ParquetFile(outputFolder, new File("year=2011/month=12/day=06/server=xxx/f1.parquet")));
    assertEquals("[2011-12-06]", job.getDaysCovered().toString());

    assertThat(job.getParquetFiles().size(), is(2));
    assertThat(job.getDaysCovered(), is(Lists.newArrayList(LocalDate.of(2011, 12, 06))));
    assertThat(job.getTotalParquetBytes(), is(0L));

    job.addParquetFile(new ParquetFile(outputFolder, new File("year=2011/month=12/day=07/server=xxx/f1.parquet")));

    assertThat(job.getParquetFiles().size(), is(3));
    assertThat(job.getDaysCovered(), is(Lists.newArrayList(
        LocalDate.of(2011, 12, 06),
        LocalDate.of(2011, 12, 07))));
    assertThat(job.getTotalParquetBytes(), is(0L));

    job.addParquetFile(new ParquetFile(outputFolder, new File("year=2011/month=12/day=05/server=xxx/f1.parquet")));
    assertEquals("[2011-12-05, 2011-12-06, 2011-12-07]", job.getDaysCovered().toString());

    assertThat(job.getParquetFiles().size(), is(4));
    assertThat(job.getDaysCovered(), is(Lists.newArrayList(
        LocalDate.of(2011, 12, 05),
        LocalDate.of(2011, 12, 06),
        LocalDate.of(2011, 12, 07)
    )));
    assertThat(job.getTotalParquetBytes(), is(0L));

    job.addParquetFile(new ParquetFile(outputFolder, new File("year=2011/month=12/day=05/server=xxx/f4.parquet")));

    assertThat(job.getParquetFiles().size(), is(5));
    assertThat(job.getDaysCovered(), is(Lists.newArrayList(
        LocalDate.of(2011, 12, 05),
        LocalDate.of(2011, 12, 06),
        LocalDate.of(2011, 12, 07)
    )));
    assertThat(job.getTotalParquetBytes(), is(0L));
  }

  @Test
  public void testParquetBytes() {
    LocalConversionJob job = new LocalConversionJob(serverInfo, pcapFiles, outputFolder);
    logger.info("job = {}", job);

    ParquetFile parquetFile = mock(ParquetFile.class);
    when(parquetFile.size()).thenReturn(22L);
    when(parquetFile.getDate()).thenReturn(LocalDate.of(2011, 12, 06));
    job.addParquetFile(parquetFile);

    assertThat(job.getParquetFiles().size(), is(1));
    assertThat(job.getDaysCovered(), is(Lists.newArrayList(LocalDate.of(2011, 12, 06))));
    assertThat(job.getTotalParquetBytes(), is(22L));
  }

}