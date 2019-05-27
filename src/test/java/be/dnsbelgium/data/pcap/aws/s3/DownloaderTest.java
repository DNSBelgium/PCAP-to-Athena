package be.dnsbelgium.data.pcap.aws.s3;

import be.dnsbelgium.data.pcap.utils.FileSize;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
@TestPropertySource(locations="classpath:test-application.properties")
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
/*
 *
 * You can run this test by specifying "aws-integration-tests" as value for system property test-groups
 *
 * For example:
 *
 *   mvn -Dtest=DownloaderTest test -Dtest-groups=aws-integration-tests
 */
public class DownloaderTest {

  private static final Logger logger = getLogger(DownloaderTest.class);

  @SuppressWarnings({"unused", "SpringJavaInjectionPointsAutowiringInspection"})
  @Autowired
  private AmazonS3 amazonS3;

  @Autowired
  private Downloader downloader;

  @Autowired
  private Uploader uploader;

  @Value("${pcap.bucket.name}")
  private String pcapBucketName;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File downloadFolder;
  private File localFile;

  final static String UNCOMPRESSED_PCAP_S3_KEY = "pcap/server=serverName/year=2018/month=10/day=30/1540940413_serverName.dns.be.p2p2.pcap2736_DONE";
  final static String COMPRESSED_PCAP_S3_KEY   = "pcap/server=serverName/year=2018/month=10/day=30/1540940413_serverName.dns.be.p2p2.pcap2736_DONE.gz";

  @Before
  public void before() throws IOException {
    logger.info("pcapBucketName = {}", pcapBucketName);
    logger.info("downloadFolder = {}", downloadFolder);
    downloadFolder = temporaryFolder.newFolder();

    File dns1 = ResourceUtils.getFile("classpath:pcap/dns1.pcap");
    File dns2 = ResourceUtils.getFile("classpath:pcap/dns2.pcap.gz");

    // Upload dummy pcap files
    uploader.upload(pcapBucketName, UNCOMPRESSED_PCAP_S3_KEY, dns1);
    uploader.upload(pcapBucketName, COMPRESSED_PCAP_S3_KEY, dns2);

    amazonS3.listObjects(pcapBucketName, "pcap").getObjectSummaries().stream().map(S3ObjectSummary::getKey).forEach(System.out::println);

    // Verify
    Assert.assertThat(amazonS3.listObjects(pcapBucketName, "pcap").getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        hasItem(UNCOMPRESSED_PCAP_S3_KEY));
    Assert.assertThat(amazonS3.listObjects(pcapBucketName, "pcap").getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        hasItem(COMPRESSED_PCAP_S3_KEY));
  }

  @After
  public void after() throws IOException {
    // Delete dummy pcap files
    amazonS3.deleteObject(pcapBucketName, UNCOMPRESSED_PCAP_S3_KEY);
    amazonS3.deleteObject(pcapBucketName, COMPRESSED_PCAP_S3_KEY);

    // Verify
    Assert.assertThat(amazonS3.listObjects(pcapBucketName).getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        not(contains(UNCOMPRESSED_PCAP_S3_KEY)));
    Assert.assertThat(amazonS3.listObjects(pcapBucketName).getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        not(contains(COMPRESSED_PCAP_S3_KEY)));
  }

  @Test
  public void downloadUncompressedFile() throws IOException {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName(pcapBucketName);
    summary.setKey(UNCOMPRESSED_PCAP_S3_KEY);
    summary.setLastModified(new Date());
    summary.setSize(26210);

    S3PcapFile s3PcapFile = S3PcapFile.parse(summary);
    s3PcapFile.setDownloadFolder(downloadFolder);

    localFile = downloader.download(summary, s3PcapFile.getLocalFile());

    logger.info("localFile size = {}", localFile.length());
    logger.info("localFile modified = {}", localFile.lastModified());
    logger.info("fileSize: {}", FileSize.friendlySize(localFile.length()));

    assertTrue("local file should exist", localFile.exists());
    assertTrue("local file should be a file", localFile.isFile());
    assertTrue("local file should readable", localFile.canRead());
    assertEquals("1540940413_serverName.dns.be.p2p2.pcap2736_DONE.pcap", localFile.getName());
    assertEquals(269, localFile.length());
    assertEquals(downloadFolder.getAbsolutePath(), localFile.getParent());
  }

  @Test
  public void downloadCompressedFile() throws IOException {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName(pcapBucketName);
    summary.setKey(COMPRESSED_PCAP_S3_KEY);
    summary.setLastModified(new Date());
    summary.setSize(26210);

    S3PcapFile s3PcapFile = S3PcapFile.parse(summary);
    s3PcapFile.setDownloadFolder(downloadFolder);

    localFile = downloader.download(summary, s3PcapFile.getLocalFile());

    logger.info("localFile size = {}", localFile.length());
    logger.info("localFile modified = {}", localFile.lastModified());
    logger.info("fileSize: {}", FileSize.friendlySize(localFile.length()));
    assertTrue("local file should exist", localFile.exists());
    assertTrue("local file should be a file", localFile.isFile());
    assertTrue("local file should readable", localFile.canRead());
    assertEquals("1540940413_serverName.dns.be.p2p2.pcap2736_DONE.gz.pcap.gz", localFile.getName());
    assertEquals(downloadFolder.getAbsolutePath(), localFile.getParent());
    assertEquals(205, localFile.length());
  }

  //@Test
  //@Ignore
  public void restoreFromGlacier() {
    String bucket = "some-bucket";
    String prefix = "antwerpen1.dns.be/06-10-2018";

    List<S3ObjectSummary> files = downloader.findGlacierFilesIn(bucket, prefix);
    logger.info("Files in Glacier: {}", files.size());

    int requests = downloader.requestRestoreFromGlacierByPrefix(bucket, prefix, 30);
    logger.info("Restore requests = {}", requests);

    //downloader.requestRestoreFromGlacier(bucket, prefix, 30);
//    files = downloader.findGlacierFilesIn(bucket, prefix);
//    logger.info("Files in Glacier: {}", files.size());

  }





}