package be.dnsbelgium.data.pcap.aws.s3;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Test;
import org.slf4j.Logger;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("ConstantConditions")
public class S3PcapFileTest {

  private static final Logger logger = getLogger(S3PcapFileTest.class);


  @Test
  public void testContructor() {
    String key = "prefix/server=amsterdam1.dns.be/year=2018/month=06/day=01/1527804007_amsterdam1.dns.be.p2p2.pcap4249_DONE";
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("random");
    summary.setKey(key);
    S3PcapFile file = S3PcapFile.parse(summary);
    assertEquals("amsterdam1.dns.be", file.getServer());
    assertEquals(LocalDate.of(2018, 6, 1), file.getDate());
    assertFalse(file.isCompressed());
  }

  @Test
  public void testContructorWithoutPrefix() {
    String key = "server=amsterdam1.dns.be/year=2018/month=06/day=01/1527804007_amsterdam1.dns.be.p2p2.pcap4249_DONE";
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("random");
    summary.setKey(key);
    S3PcapFile file = S3PcapFile.parse(summary);
    assertEquals("amsterdam1.dns.be", file.getServer());
    assertEquals(LocalDate.of(2018, 6, 1), file.getDate());
    assertFalse(file.isCompressed());
  }

  @Test
  public void testContructorOldPattern() {
    String key = "amsterdam1.dns.be/01-06-2018/1527804007_amsterdam1.dns.be.p2p2.pcap4249_DONE";
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("random");
    summary.setKey(key);
    S3PcapFile file = S3PcapFile.parse(summary);
    assertEquals("amsterdam1.dns.be", file.getServer());
    assertEquals(LocalDate.of(2018, 6, 1), file.getDate());
    assertFalse(file.isCompressed());
  }

  @Test
  public void compressed() {
    String key = "amsterdam1.dns.be/25-12-2017/1527804007_amsterdam1.dns.be.p2p2.pcap0049_DONE.gz";
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("random");
    summary.setKey(key);
    S3PcapFile file = S3PcapFile.parse(summary);
    assertEquals("amsterdam1.dns.be", file.getServer());
    assertEquals(LocalDate.of(2017, 12, 25), file.getDate());
    assertEquals("1527804007_amsterdam1.dns.be.p2p2.pcap0049_DONE.gz", file.getFileName());
    assertEquals(1527804007, file.getInstant().getEpochSecond());
    assertEquals(summary, file.getObjectSummary());
    assertTrue(file.isCompressed());
  }

  private S3ObjectSummary makeSummary(String key) {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("random");
    summary.setKey(key);
    return summary;
  }

  private S3PcapFile makeFile(String key) {
    return S3PcapFile.parse(makeSummary(key));
  }

  @Test
  public void sort() {
    List<S3PcapFile> files =new ArrayList<>();
    S3PcapFile file1 = makeFile("amsterdam1.dns.be/25-12-2017/1527804017_amsterdam1.dns.be.p2p2.pcap0049_DONE.gz");
    S3PcapFile file2 = makeFile("brussels01.dns.be/25-12-2017/1527804001_brussels01.dns.be.p2p2.pcap0049_DONE.gz");
    S3PcapFile file3 = makeFile("amsterdam1.dns.be/25-12-2017/1527804005_amsterdam1.dns.be.p2p2.pcap0049_DONE.gz");
    S3PcapFile file4 = makeFile("amsterdam1.dns.be/25-12-2017/1527804007_amsterdam1.dns.be.p2p2.pcap0049_DONE.gz");
    files.add(file1);
    files.add(file2);
    files.add(file3);
    files.add(file4);
    Collections.sort(files);
    assertEquals(file3, files.get(0));
    assertEquals(file4, files.get(1));
    assertEquals(file1, files.get(2));
    assertEquals(file2, files.get(3));
  }

  @Test
  public void sort2() {
    List<S3PcapFile> files =new ArrayList<>();
    S3PcapFile file1 = makeFile("amsterdam1.dns.be/25-12-2017/1527804017_amsterdam1.dns.be.p2p2.pcap0008_DONE.gz");
    S3PcapFile file2 = makeFile("amsterdam1.dns.be/25-12-2017/1527804001_amsterdam1.dns.be.em1.pcap0049_DONE.gz");
    S3PcapFile file3 = makeFile("amsterdam1.dns.be/25-12-2017/1527804005_amsterdam1.dns.be.p2p2.pcap0009_DONE.gz");
    S3PcapFile file4 = makeFile("amsterdam1.dns.be/25-12-2017/1527804007_amsterdam1.dns.be.em1.pcap0050_DONE.gz");
    S3PcapFile file5 = makeFile("amsterdam1.dns.be/25-12-2017/1527804007_amsterdam1.dns.be.pcap0049_DONE.gz");
    files.add(file1);
    files.add(file2);
    files.add(file3);
    files.add(file4);
    files.add(file5);
    Collections.sort(files);
    assertEquals(file2, files.get(0));
    assertEquals(file4, files.get(1));
    assertEquals(file3, files.get(2));
    assertEquals(file5, files.get(3));
    assertEquals(file1, files.get(4));
  }

  @Test
  public void newFolderStructure() {
    String key = "prefix/server=dummy1.dns.com/year=2017/month=12/day=25/123_server.be.interface.pcap605_DONE.gz";
    S3PcapFile file = makeFile(key);
    logger.info("file = {}", file);
    assertEquals(LocalDate.of(2017, 12, 25), file.getDate());
    assertEquals("123_server.be.interface.pcap605_DONE.gz", file.getFileName());
    assertEquals(123, file.getInstant().getEpochSecond());
    assertEquals("interface", file.getNetworkInterface());
    assertEquals("605", file.getSequenceNr());
    String improvedKey = file.improvedKey("prefix", new ServerInfo("dummy1.dns.com"));
    assertEquals(key, improvedKey);
    String expected = key
        .replace("prefix", "new-prefix")
        .replace("dummy1.dns.com", "other.server");
    assertEquals(expected, file.improvedKey("new-prefix", new ServerInfo("other.server")));
  }

  @Test
  public void newFolderStructureNoInterface() {
    String key = "prefix/server=dummy1.dns.com/year=2017/month=12/day=25/123_server.be.pcap605_DONE.gz";
    S3PcapFile file = makeFile(key);
    logger.info("file = {}", file);
    assertEquals(LocalDate.of(2017, 12, 25), file.getDate());
    assertEquals("123_server.be.pcap605_DONE.gz", file.getFileName());
    assertEquals(123, file.getInstant().getEpochSecond());
    assertNull(file.getNetworkInterface());
    assertNull(file.getSequenceNr());
  }


  @Test
  public void improvedKey() {
    S3PcapFile file1 = makeFile("some.server.com/25-12-2017/123_yyyy.zzz_DONE");
    String improved = file1.improvedKey("prefix", new ServerInfo("dummy1.dns.com"));
    logger.info("improved = {}", improved);
    assertEquals("prefix/server=dummy1.dns.com/year=2017/month=12/day=25/123_yyyy.zzz_DONE", improved);
  }

}