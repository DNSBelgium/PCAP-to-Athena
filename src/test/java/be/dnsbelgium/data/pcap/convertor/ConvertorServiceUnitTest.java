package be.dnsbelgium.data.pcap.convertor;

import be.dnsbelgium.data.pcap.aws.athena.AthenaTools;
import be.dnsbelgium.data.pcap.aws.s3.*;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.slf4j.LoggerFactory.getLogger;

public class ConvertorServiceUnitTest {

  private static final Logger logger = getLogger(ConvertorServiceUnitTest.class);

  private ConvertorService convertorService;
  private Downloader downloader = mock(Downloader.class);
  private Uploader uploader = mock(Uploader.class);
  private Tagger tagger = mock(Tagger.class);
  private PcapConvertor convertor = mock(PcapConvertor.class);
  private FileHelper fileHelper = mock(FileHelper.class);
  private AthenaTools athena = mock(AthenaTools.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final static String PCAP_BUCKET = "s3-bucket-with-pcaps";
  private final static String PARQUET_BUCKET = "s3-bucket-with-parquet-files";
  private final static String ARCHIVE_BUCKET = "s3-bucket-with-archived-pcap-files";
  private final static String ARCHIVE_PREFIX = "archive";
  private final static String PARQUET_PREFIX = RandomStringUtils.randomAlphanumeric(5);
  private static final String PARQUET_REPO_NAME = "dnsdata-repo";
  private static final String ATHENA_DATABASE_NAME = "dummydb";
  private static final String ATHENA_TABLE_NAME = "dummytable";

  private File outputFolder, downloadFolder;

  private List<File> localParquetFiles;

  private S3PcapFile pcapFile1, pcapFile2;
  private List<S3PcapFile> pcapFiles;

  private LocalDate day_2018_11_25 = LocalDate.of(2018, 11, 25);
  private LocalDate day_2018_11_26 = LocalDate.of(2018, 11, 26);
  private LocalDate day_2018_11_27 = LocalDate.of(2018, 11, 27);

  private final String parquet1 = "year=2018/month=11/day=26/server=dummy/parquet-file1.parquet";
  private final String parquet2 = "year=2018/month=11/day=26/server=dummy/parquet-file2.parquet";
  private final String parquet3 = "year=2018/month=11/day=27/server=dummy/parquet-file3.parquet";
  private ParquetFile parquetFile1, parquetFile2, parquetFile3;

  private S3ObjectSummary summary1, summary2;

  private ConversionJob job;
  private ServerInfo serverInfo = new ServerInfo("dummy.example.com", "dummy", "honolulu");

  @Before
  public void before() throws IOException {
    outputFolder = temporaryFolder.newFolder("parquet-output");
    downloadFolder = temporaryFolder.newFolder("pcap-downloads");

    summary1 = makeSummary(PCAP_BUCKET, "dummy.example.com/26-11-2018/12345_dummy.pcap.gz_DONE");
    summary2 = makeSummary(PCAP_BUCKET, "dummy.example.com/26-11-2018/45678_dummy.pcap.gz_DONE");

    pcapFile1 = S3PcapFile.parse(summary1);
    pcapFile2 = S3PcapFile.parse(summary2);
    pcapFiles = Lists.newArrayList(pcapFile1, pcapFile2);

    parquetFile1 = new ParquetFile(outputFolder, new File(outputFolder, parquet1));
    parquetFile2 = new ParquetFile(outputFolder, new File(outputFolder, parquet2));
    parquetFile3 = new ParquetFile(outputFolder, new File(outputFolder, parquet3));

    job = new ConversionJob(serverInfo, day_2018_11_26, outputFolder);

    localParquetFiles = new ArrayList<>();
    localParquetFiles.add(new File(outputFolder, parquet1));
    localParquetFiles.add(new File(outputFolder, parquet2));
    localParquetFiles.add(new File(outputFolder, parquet3));

    List<String> serverNames = Lists.newArrayList("myserver1", "myserver2");

    ConvertorConfig config = new ConvertorConfig(
        PCAP_BUCKET,
        PARQUET_BUCKET,
        ARCHIVE_BUCKET,
        ARCHIVE_PREFIX,
        true,
        downloadFolder.getAbsolutePath(),
        outputFolder.getAbsolutePath(),
        "",
        true,
        true,
        PARQUET_PREFIX,
        PARQUET_REPO_NAME,
        serverNames,
        ATHENA_DATABASE_NAME,
        ATHENA_TABLE_NAME
    );
    convertorService = new ConvertorService(config, downloader, uploader, tagger, convertor, fileHelper, athena);
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(downloadFolder);
    FileUtils.deleteDirectory(outputFolder);
  }

  @SuppressWarnings("SameParameterValue")
  private S3ObjectSummary makeSummary(String bucket, String key) {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName(bucket);
    summary.setKey(key);
    return summary;
  }

  @Test
  @Ignore // TODO: rework this test
  public void uploadFails() throws IOException, InterruptedException {
    S3ObjectSummary summary = makeSummary(PCAP_BUCKET, "dummy/25-11-2018/12345_dummy.pcap.gz_DONE");
    List<S3PcapFile> pcapFiles = Lists.newArrayList(S3PcapFile.parse(summary));

    when(downloader.download(summary, downloadFolder)).thenReturn(new File(downloadFolder, summary.getKey()));
    when(fileHelper.uniqueSubFolder(anyString())).thenReturn(new File(outputFolder + "dummy"));
    when(fileHelper.findRecursively(any(File.class), anyString())).thenReturn(localParquetFiles);
    when(uploader.upload(anyString(), anyString(), any(File.class))).thenReturn(true);
    when(uploader.upload(anyString(), anyString(), any(File.class))).thenReturn(false);

    verify(downloader).download(summary, downloadFolder);
    verify(uploader).upload(PARQUET_BUCKET, PARQUET_PREFIX + "/dnsdata/" + parquet1, localParquetFiles.get(0));
    verify(uploader, never()).upload(PARQUET_BUCKET, PARQUET_PREFIX + "/dnsdata/" + parquet2, localParquetFiles.get(1));
    verify(uploader, never()).upload(PARQUET_BUCKET, PARQUET_PREFIX + "/dnsdata/" + parquet3, localParquetFiles.get(2));
    verify(downloader, never()).move(PCAP_BUCKET, summary.getKey(), ARCHIVE_BUCKET, summary.getKey());
    verify(fileHelper, never()).delete(new File(downloadFolder + "/" + summary.getKey()));

    ArgumentCaptor<S3PcapFile> pcapFileCaptor = ArgumentCaptor.forClass(S3PcapFile.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<Map<String, String>> tagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(tagger, atLeast(2)).addTags(pcapFileCaptor.capture(), tagsCaptor.capture());

    File pcap = new File(downloadFolder, summary.getKey());

    verify(convertor).convertToParquet(job.getLocalConversionJob());

    logger.info("first set of tags: {}", tagsCaptor.getAllValues().get(0));
    logger.info("second set of tags: {}", tagsCaptor.getAllValues().get(1));

    assertEquals("BUSY", tagsCaptor.getAllValues().get(0).get("CONVERSION_STATUS"));
    assertNull("No CONVERSION_ENDED in first tagging", tagsCaptor.getAllValues().get(0).get("CONVERSION_ENDED"));

    Instant started = Instant.parse(tagsCaptor.getAllValues().get(0).get("CONVERSION_STARTED"));
    assertTrue("CONVERSION_STARTED should be in the past", started.isAfter(Instant.now().minusSeconds(30)));
    assertTrue("CONVERSION_STARTED should be in near past", started.isBefore(Instant.now().plusSeconds(10)));

    assertEquals("FAILED", tagsCaptor.getAllValues().get(1).get("CONVERSION_STATUS"));
    assertNotNull("CONVERSION_ERROR present second tagging", tagsCaptor.getAllValues().get(1).get("CONVERSION_ERROR"));
  }

  @Test
  public void conversionFails() throws IOException, InterruptedException {
    when(downloader.listFilesIn(PCAP_BUCKET, "dummy.example.com/25-11-2018/")).thenReturn(pcapFiles);
    when(downloader.listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=25/")).thenReturn(pcapFiles);

    ConversionJob job = new ConversionJob(serverInfo, day_2018_11_25, outputFolder);

    File pcap1 = new File(downloadFolder, summary1.getKey());
    File pcap2 = new File(downloadFolder, summary2.getKey());
    when(downloader.download(summary1, downloadFolder)).thenReturn(pcap1);
    when(downloader.download(summary2, downloadFolder)).thenReturn(pcap2);

    when(downloader.download(summary1, pcapFile1.getLocalFile())).thenReturn(simulateDownload(pcapFile1));
    when(downloader.download(summary2, pcapFile2.getLocalFile())).thenReturn(simulateDownload(pcapFile2));

    when(fileHelper.uniqueSubFolder(anyString())).thenReturn(new File(outputFolder + "dummy"));
    when(fileHelper.findRecursively(any(File.class), anyString())).thenReturn(localParquetFiles);

    when(convertor.convertToParquet(job.getLocalConversionJob())).thenThrow(new InterruptedException("something went boom"));
    when(uploader.upload(anyString(), anyString(), any(File.class))).thenReturn(true);

    convertorService.execute(job);
    assertEquals(ConversionJob.Status.FAILED, job.getStatus());

    verify(downloader).listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=25/");
    verify(downloader).download(summary1, pcapFile1.getLocalFile());
    verify(downloader).download(summary2, pcapFile2.getLocalFile());
    verify(uploader, never()).upload(anyString(), anyString(), any(File.class));
    verify(downloader, never()).move(anyString(), anyString(), anyString(), anyString());
    verify(fileHelper, never()).delete(any(File.class));

    ArgumentCaptor<S3PcapFile> pcapFileCaptor = ArgumentCaptor.forClass(S3PcapFile.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<Map<String, String>> tagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(tagger, atLeast(2)).addTags(pcapFileCaptor.capture(), tagsCaptor.capture());

    verify(convertor).convertToParquet(job.getLocalConversionJob());

    // TODO: is FAILED should be the status ?
    assertEquals("BUSY",   tagsCaptor.getAllValues().get(0).get("CONVERSION_STATUS"));
    assertEquals("BUSY", tagsCaptor.getAllValues().get(1).get("CONVERSION_STATUS"));
    assertNull("No CONVERSION_ENDED in first tagging",  tagsCaptor.getAllValues().get(0).get("CONVERSION_ENDED"));
    assertNull("No CONVERSION_ENDED in second tagging", tagsCaptor.getAllValues().get(1).get("CONVERSION_ENDED"));

    Instant started = Instant.parse(tagsCaptor.getAllValues().get(0).get("CONVERSION_STARTED"));
    assertTrue("CONVERSION_STARTED should be in the past",  started.isAfter(Instant.now().minusSeconds(30)));
    assertTrue("CONVERSION_STARTED should be in near past", started.isBefore(Instant.now().plusSeconds(10)));
  }

  @Test
  public void findPcapFiles() {
    when(downloader.listFilesIn(PCAP_BUCKET, "dummy.example.com/26-11-2018/")).thenReturn(pcapFiles);
    when(downloader.listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=26/")).thenReturn(pcapFiles);

    ConversionJob job = new ConversionJob(serverInfo, day_2018_11_26, outputFolder);
    job.logStatus();
    assertEquals(ConversionJob.Status.INITIAL, job.getStatus());

    convertorService.findPcapFiles(job);
    verify(downloader).listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=26/");

    assertEquals(ConversionJob.Status.PCAP_FILES_LISTED, job.getStatus());
    assertEquals(2, job.getPcapFiles().size());
    assertEquals(pcapFile1, job.getPcapFiles().get(0));
    assertEquals(pcapFile2, job.getPcapFiles().get(1));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void downloadPcapFiles() throws IOException {
    // This test supports both old-style as new-style folder format
    when(downloader.listFilesIn(PCAP_BUCKET, "dummy.example.com/26-11-2018/")).thenReturn(pcapFiles);
    when(downloader.listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=26/")).thenReturn(pcapFiles);

    job.logStatus();
    convertorService.findPcapFiles(job);
    assertEquals(ConversionJob.Status.PCAP_FILES_LISTED, job.getStatus());

    when(downloader.download(summary1, pcapFile1.getLocalFile())).thenReturn(simulateDownload(pcapFile1));
    when(downloader.download(summary2, pcapFile2.getLocalFile())).thenReturn(simulateDownload(pcapFile2));

    convertorService.downloadPcapFiles(job);
    assertEquals(ConversionJob.Status.PCAP_FILES_DOWNLOADED, job.getStatus());

    verify(downloader).download(summary1, pcapFile1.getLocalFile());
    verify(downloader).download(summary2, pcapFile2.getLocalFile());

    ArgumentCaptor<Map> tags1 = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map> tags2 = ArgumentCaptor.forClass(Map.class);

    verify(tagger).addTags(eq(pcapFile1), tags1.capture());
    verify(tagger).addTags(eq(pcapFile2), tags2.capture());

    assertEquals("pcapFile1 should be tagged as BUSY", "BUSY", tags1.getValue().get("CONVERSION_STATUS"));
    assertEquals("pcapFile2 should be tagged as BUSY", "BUSY", tags2.getValue().get("CONVERSION_STATUS"));

    assertTrue("should be have a CONVERSION_STARTED tag", tags1.getValue().containsKey("CONVERSION_STARTED"));
    assertTrue("should be have a CONVERSION_STARTED tag", tags2.getValue().containsKey("CONVERSION_STARTED"));
  }

  @Test
  public void downloadPcapFilesFails() throws IOException {
    when(downloader.listFilesIn(PCAP_BUCKET, "dummy.example.com/26-11-2018/")).thenReturn(pcapFiles);
    when(downloader.listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=26/")).thenReturn(pcapFiles);

    job.logStatus();
    convertorService.findPcapFiles(job);
    assertEquals(ConversionJob.Status.PCAP_FILES_LISTED, job.getStatus());
    when(downloader.download(summary1, downloadFolder)).thenReturn(simulateDownload(pcapFile1));
    when(downloader.download(summary2, downloadFolder)).thenReturn(new File(""));
    try {
      convertorService.downloadPcapFiles(job);
      fail("should throw IllegalStateException");
    } catch (IllegalStateException e) {
      assertEquals("Not all PCAP files downloaded. Download failed ?", e.getMessage());
    }
    assertEquals(ConversionJob.Status.PCAP_FILES_LISTED, job.getStatus());
  }

  private File simulateDownload(S3PcapFile pcapFile) throws IOException {
    pcapFile.setDownloadFolder(downloadFolder);
    FileUtils.writeStringToFile(pcapFile.getLocalFile(), "data");
    return pcapFile.getLocalFile();
  }

  @Test
  public void convertPcapFiles() throws InterruptedException {
    ConversionJob job = mock(ConversionJob.class);
    convertorService.convertPcapFiles(job);

    verify(convertor).convertToParquet(job.getLocalConversionJob());
    verify(job).markPcapFilesConverted();
  }

  private void addParquetFiles() {
    LocalConversionJob localJob = job.getLocalConversionJob();
    localJob.addParquetFile(parquetFile1);
    localJob.addParquetFile(parquetFile2);
    localJob.addParquetFile(parquetFile3);
  }

  @Test
  public void uploadParquetFiles() throws IOException {
    job.setPcapFiles(pcapFiles);
    simulateDownload(pcapFile1);
    simulateDownload(pcapFile2);
    job.markPcapsDownloaded();
    addParquetFiles();
    job.markPcapFilesConverted();
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile1))).thenReturn(true);
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile2))).thenReturn(true);
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile3))).thenReturn(true);
    convertorService.uploadParquetFiles(job);
    assertEquals(ConversionJob.Status.PARQUET_FILES_UPLOADED, job.getStatus());
    assertEquals(3, job.getUploadCount());
    logger.info("job.getDays() = {}", job.getDays());
    assertEquals("parquet files should cover 2 days", 2, job.getDays().size());
    assertTrue(job.getDays().contains(LocalDate.of(2018, 11, 26)));
    assertTrue(job.getDays().contains(LocalDate.of(2018, 11, 27)));
  }

  @Test
  public void createAthenaPartitions() throws IOException {
    job.setPcapFiles(pcapFiles);
    simulateDownload(pcapFile1);
    simulateDownload(pcapFile2);
    addParquetFiles();
    job.markPcapsDownloaded();
    job.markPcapFilesConverted();
    job.setUploadCount(3);
    convertorService.createAthenaPartitions(job);
    assertEquals(ConversionJob.Status.ATHENA_PARTITIONS_CREATED, job.getStatus());
    String expected_S3_LOCATION = convertorService.getConfig().getParquetS3Location();
    verify(athena).addPartition(day_2018_11_26, serverInfo, ATHENA_DATABASE_NAME, ATHENA_TABLE_NAME, expected_S3_LOCATION);
    verify(athena).addPartition(day_2018_11_27, serverInfo, ATHENA_DATABASE_NAME, ATHENA_TABLE_NAME, expected_S3_LOCATION);
  }

  @Test
  public void deleteLocalFiles() throws IOException {
    job.setPcapFiles(pcapFiles);
    simulateDownload(pcapFile1);
    simulateDownload(pcapFile2);
    job.markPcapsDownloaded();
    addParquetFiles();
    job.markPcapFilesConverted();
    job.setUploadCount(3);
    job.markAthenaPartitionsCreated();
    convertorService.deleteLocalFiles(job);
    verify(fileHelper).delete(pcapFile1.getLocalFile());
    verify(fileHelper).delete(pcapFile2.getLocalFile());
    verify(fileHelper, atLeastOnce()).deleteRecursively(parquetFile1.getBaseFolder());
    verify(fileHelper, atLeastOnce()).deleteRecursively(parquetFile2.getBaseFolder());
    verify(fileHelper, atLeastOnce()).deleteRecursively(parquetFile3.getBaseFolder());
    assertEquals(ConversionJob.Status.LOCAL_FILES_DELETED, job.getStatus());
  }

  @Test
  public void movePcapFilesToArchiveBucket() throws IOException {
    job.setPcapFiles(pcapFiles);
    simulateDownload(pcapFile1);
    simulateDownload(pcapFile2);
    job.markPcapsDownloaded();
    LocalConversionJob localJob = job.getLocalConversionJob();
    localJob.addParquetFile(parquetFile1);
    localJob.addParquetFile(parquetFile2);
    localJob.addParquetFile(parquetFile3);
    job.markPcapFilesConverted();
    job.setUploadCount(3);
    job.markAthenaPartitionsCreated();
    job.markLocalFilesDeleted();
    when(downloader.move(eq(PCAP_BUCKET), anyString(), eq(ARCHIVE_BUCKET), anyString())).thenReturn(true);
    convertorService.movePcapFilesToArchiveBucket(job);
    assertEquals(ConversionJob.Status.PCAP_FILES_ARCHIVED, job.getStatus());
    verify(downloader).move(PCAP_BUCKET, pcapFile1.getKey(), ARCHIVE_BUCKET, pcapFile1.improvedKey(ARCHIVE_PREFIX, serverInfo));
    verify(downloader).move(PCAP_BUCKET, pcapFile2.getKey(), ARCHIVE_BUCKET, pcapFile2.improvedKey(ARCHIVE_PREFIX, serverInfo));
  }

  @Test
  public void execute() throws IOException, InterruptedException {
    when(downloader.listFilesIn(PCAP_BUCKET, "dummy.example.com/26-11-2018/")).thenReturn(pcapFiles);
    when(downloader.listFilesIn(PCAP_BUCKET, "server=dummy.example.com/year=2018/month=11/day=26/")).thenReturn(pcapFiles);
    when(downloader.download(summary1, downloadFolder)).thenReturn(simulateDownload(pcapFile1));
    when(downloader.download(summary2, downloadFolder)).thenReturn(simulateDownload(pcapFile2));
    when(fileHelper.uniqueSubFolder(anyString())).thenReturn(outputFolder);
    when(fileHelper.findRecursively(any(File.class), anyString())).thenReturn(localParquetFiles);
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile1))).thenReturn(true);
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile2))).thenReturn(true);
    when(uploader.upload(eq(PARQUET_BUCKET), anyString(), eq(parquetFile3))).thenReturn(true);
    when(downloader.move(eq(PCAP_BUCKET), anyString(), eq(ARCHIVE_BUCKET), anyString())).thenReturn(true);
    addParquetFiles();
    convertorService.execute(job);
    assertEquals(ConversionJob.Status.PCAP_FILES_ARCHIVED, job.getStatus());
  }

}
