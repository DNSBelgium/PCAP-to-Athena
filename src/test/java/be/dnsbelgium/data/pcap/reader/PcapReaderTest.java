package be.dnsbelgium.data.pcap.reader;

import au.com.bytecode.opencsv.CSVWriter;
import be.dnsbelgium.data.pcap.handler.PacketHandler;
import be.dnsbelgium.data.pcap.handler.PcapAnalyzer;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.Packet;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.ResourceUtils;
import org.xbill.DNS.DNSInput;
import org.xbill.DNS.Name;
import org.xbill.DNS.WireParseException;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.slf4j.LoggerFactory.getLogger;

// TODO quentinl not asserting anything yet
public class PcapReaderTest {

  private static final Logger logger = getLogger(PcapReaderTest.class);

  private static final int DEFAULT_PCAP_READER_BUFFER_SIZE = 65536;

  //
  // These tests expose a bug in nl.sidn.dnslib.message.util.DNSStringUtil#readName  //
  // see https://github.com/SIDN/entrada/pull/78

  byte[] bytes = new byte[] {
      -117, 117, -128, 0, 0, 1, 0, 0, 0, 7, 0, 1, 3, 119, 119, 119, 20, 108, 101, 115, 100, 111, 117, 99, 101, 117,
      114, 115, 100, 97, 109, 97, 110, 100, 105, 110, 101, 2, 98, 101, 0, 0, 1, 0, 1, -64, 16, 0, 2, 0, 1, 0, 1, 81,
      -128, 0, 23, 4, 100, 110, 115, 51,
      13, 49, 50, 51, 104, 106, 101, 109, 109, 101, 115, 105, 100, 101, 2, 100, 107, 0, -64, 16, 0, 2, 0, 1, 0, 1, 81,
      -128, 0, 7, 4, 100, 110, 115, 50, -64, 62, -64, 16, 0, 2, 0, 1, 0, 1, 81, -128, 0, 7, 4, 100, 110, 115, 49, -64,
      62, 32, 98, 97, 49, 52, 49, 115, 110, 114, 110, 111, 101, 49, 114, 99, 57, 109, 100, 100, 103, 114, 101, 115,
      116, 50, 51, 103, 54, 53, 55, 114, 105, 114, -64, 37, 0, 50, 0, 1, 0, 0, 2, 88, 0, 39, 1, 1, 0, 5, 4, 26, 78,
      -101, 108, 20, 90, -126, 114, -88, -42, 57, 101, -37, -11, -94, 110, 11, -71, 106, -84, -3, -120, 88, 114, -37,
      0, 7, 34, 0, 0, 0, 0, 2, -112, -64, 118, 0, 46, 0, 1, 0, 0, 2, 88, 0, -106, 0, 50, 8, 2, 0, 0, 2, 88, 92, 115,
      -18, -4, 92, 102, -73, -106, 9, -82, 2, 98, 101, 0, 120, 36, 32, 82, 100, -31, 42, -93, -60, -34, -41, 78, 4, 16,
      119, 48, -36, 110, -56, -81, 67, 114, -78, -23, -107, 28, 56, -107, 110, -5, 39, 13, 23, -101, -3, -95, 103, 75,
      41, 3, -72, 111, -93, -26, -86, -26, -29, 29, 43, 4, 64, 19, -38, -55, -4, 57, 20, -121, -12, -90, -37, 80, -63,
      -68, -92, -120, 112, -82, -3, -50, -126, 64, -116, 36, 19, 27, 103, -101, 81, 43, -60, -98, 68, -40, 77, -65, 82,
      -104, 47, 67, -83, -123, 2, 82, 7, 111, -126, -108, 69, -103, 81, -92, 81, -91, 69, -77, -41, -125, -65, 68, -32,
      31, 23, -18, 69, 88, -91, -17, -15, -118, -12, 14, -58, 21, -62, 93, 82, 120, 32, 103, 113, 50, 115, 97, 53, 117,
      101, 102, 97, 113, 104, 103, 109, 56, 107, 117, 52, 53, 112, 112, 107, 98, 111, 49, 56, 111, 49, 48, 99, 98, 114,
      -64, 37, 0, 50, 0, 1, 0, 0, 2, 88, 0, 38, 1, 1, 0, 5, 4, 26, 78, -101, 108, 20, -122, -123, -45, -47, 112, 114,
      -97, 123, -82, -48, -53, 31, -10, -87, -93, 22, -13, 53, -49, -88, 0, 6, 32, 0, 0, 0, 0, 18, -63, 108, 0, 46, 0,
      1, 0, 0, 2, 88, 0, -106, 0, 50, 8, 2, 0, 0, 2, 88, 92, 107, 117, -111, 92, 94, 61, -128, 9, -82, 2, 98, 101, 0,
      91, -42, -87, -18, 62, 17, -128, -66, 117, -60, 113, -14, -89, 125, 12, 116, -32, -115, -77, 80, 73, 64, 81, 36,
      97, 123, 60, 105, 20, 78, 7, -112, 25, 38, 83, -127, 59, -31, 9, -7, -63, 46, -46, 99, -31, -123, -8, -32, 45,
      -74, 47, 116, -93, 11, 15, 88, 2, 93, 9, 44, 15, 104, -118, 105, 45, 78, -91, -123, 52, 37, -124, -66, 53, 4, -54,
      -51, -59, 4, 84, 108, 8, 28, 29, -82, -28, 56, -27, -128, -23, 56, 10, -37, 30, 61, -36, -74, 28, -127, -9, 94, 4,
      -112, 98, -120, -128, 55, -5, -20, 80, 12, -59, 58, 126, -7, 49, -60, -106, -123, 85, -70, -9, 38, -89, 34, 95,
      28, -89, -34, 0, 0, 41, 16, 0, 0, 0, -128, 0, 0, 0};


  @Test
  public void readCompressedName() {
    System.out.println("bytes = " + bytes.length);
    NetworkData networkData = new NetworkData(bytes);
    Message dnsMessage = new Message(networkData, true);
    System.out.println("dnsMessage = " + dnsMessage);
  }

  @Test
  public void testNSRecord() throws IOException {
    System.out.println("bytes = " + bytes.length);
    org.xbill.DNS.Message message = new org.xbill.DNS.Message(bytes);
    System.out.println("message = " + message);
  }

  @Test
  public void singleName() throws WireParseException {
    DNSInput in = new DNSInput(bytes);
    in.jump(92);
    in.setActive(7);
    Name singleName = new Name(in);
    System.out.println("singleName = " + singleName);

    in.jump(111);
    in.setActive(7);
    singleName = new Name(in);
    System.out.println("singleName = " + singleName);

    in.jump(57);
    in.setActive(23);
    singleName = new Name(in);
    System.out.println("singleName = " + singleName);
  }


  @Test
  @Ignore // takes too long for a unit test and requires a 34MB PCAP file which we will not push to git)
  public void readBigPcapFileAmsterdam() throws IOException {
    String fileName = "pcap/1550311273_amsterdam1.dns.be.p2p2.pcap0556_DONE.gz";
    analyzePcap(fileName);
  }

  @Test
  public void readSmallPcapFile() throws IOException {
    File dns1 = ResourceUtils.getFile("classpath:pcap/dns1.pcap");
    analyzePcap(dns1.getAbsolutePath());
  }

  public void toCSv(String inputFile, String csvFileName) throws IOException {

    CSVWriter writer = new CSVWriter(new FileWriter(csvFileName));
    AtomicInteger counter = new AtomicInteger(0);
    processPcap(inputFile, packet -> {
      String src = packet.getSrc();
      int port = packet.getSrcPort();
      long ts = packet.getTs();
      int payloadLength = packet.getPayloadLength();
      String messageType = "";
      String qName = "";
      int id = -1;
      int msgCount = 0;
      int questionCount = 0;
      if (packet instanceof DNSPacket) {
        DNSPacket dnsPacket = (DNSPacket) packet;
        messageType = dnsPacket.getMessage().getHeader().getQr().toString();
        id = dnsPacket.getMessage().getHeader().getId();
        msgCount = dnsPacket.getMessages().size();
        questionCount = dnsPacket.getMessage().getQuestions().size();
        if (questionCount > 0) {
          qName = dnsPacket.getMessage().getQuestions().get(0).getqName();
        } else {
          logger.info("dnsPacket has no questions = {}", dnsPacket.getMessageCount());
        }
      }
      String[] data = new String[]{src, str(port), str(ts), str(payloadLength), messageType, qName, str(id), str(msgCount), str(questionCount) };
      writer.writeNext(data);
      counter.incrementAndGet();
      return true;
    });
    writer.flush();
    logger.info("written {} records to {}", counter, csvFileName);
  }

  private String str(long value) {
    return "" + value;
  }

  @Test
  public void toCsvWithSmallFile() throws IOException {
    ClassPathResource resource = new ClassPathResource("pcap/dns1.pcap");
    File pcapFile = resource.getFile();
    toCSv(pcapFile.getAbsolutePath(), "dns1.csv");
  }

  @Test
  public void testCsvWithBigFile() throws IOException {
    String fileName = System.getProperty("user.home") + "/devel/experiments/pcap-not-in-git/1550311273_amsterdam1.dns.be.p2p2.pcap0556_DONE.gz";
    File file = new File(fileName);
    if (file.exists()) {
      toCSv(fileName, "1550311273_amsterdam1.dns.be.p2p2.pcap.csv");
    } else {
      logger.info("TEST SKIPPED because file is not found: " + fileName);
      logger.info("Download it from S3 or change the code if you want to test with a bigger file");
    }
  }

  private void processPcap(String fileName, PacketHandler handler) {
    PcapReader pcapReader = new PcapReader();
    File file = new File(fileName);
    logger.info("Start loading queue from file:" + file);
    try {
      FileInputStream fis = FileUtils.openInputStream(file);
      InputStream decompressor = getDecompressorStreamWrapper(fis, fileName, DEFAULT_PCAP_READER_BUFFER_SIZE);

      DataInputStream dis = new DataInputStream(decompressor);
      pcapReader.init(dis);
    } catch (IOException e) {
      logger.error("Error opening pcap file: " + file, e);
      throw new RuntimeException("Error opening pcap file: " + file);
    }
    logger.info("handling all packets in PCAP file {}", fileName);
    for (Packet packet : pcapReader) {
      try {
        if (!handler.handle(packet)) {
          logger.warn("handler indicated to stop processing further packets");
          break;
        }
      } catch (Exception e) {
        logger.error("exception while handling packet: " + e.getMessage(), e);
      }
    }
  }

  public void analyzePcap(String fileName) throws IOException {
    PcapAnalyzer analyzer = new PcapAnalyzer();
    processPcap(fileName, analyzer);
    analyzer.logAnalysis();
  }


  /**
   * wraps the inputstream with a decompressor based on a filename ending
   *
   * @param in The input stream to wrap with a decompressor
   * @param filename The filename from which we guess the correct decompressor
   * @return the compressor stream wrapped around the inputstream. If no decompressor is found,
   *         returns the inputstream as-is
   */
  public InputStream getDecompressorStreamWrapper(InputStream in, String filename, int bufSize)
      throws IOException {
    String filenameLower = filename.toLowerCase();
    if (filenameLower.endsWith(".xz")) {
      return new XZCompressorInputStream(in);
    }
    if (filenameLower.endsWith(".pcap")) {
      return in;
    }
    if (filenameLower.endsWith(".gz")) {
      return new GZIPInputStream(in, bufSize);
    }

    throw new IOException("Could not open file with unknown extension: " + filenameLower);
  }

}
