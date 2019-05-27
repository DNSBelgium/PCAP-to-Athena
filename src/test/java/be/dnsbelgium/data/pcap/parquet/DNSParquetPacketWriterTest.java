package be.dnsbelgium.data.pcap.parquet;

import be.dnsbelgium.data.pcap.ip.SubnetChecks;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import be.dnsbelgium.data.pcap.utils.GeoLookupUtil;
import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.Question;
import nl.sidn.dnslib.types.OpcodeType;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;
import nl.sidn.pcap.decoder.IPDecoder;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.support.PacketCombination;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import parquet.Log;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.LoggerFactory.getLogger;

public class DNSParquetPacketWriterTest {

  private static final Logger logger = getLogger(DNSParquetPacketWriterTest.class);

  @Test
  public void writeParquet() throws IOException {

    ParquetLogging.removeHandlers();
    parquet.Log log = Log.getLog(parquet.hadoop.ParquetFileWriter.class);
    log.info("test");

    SubnetChecks checks = new SubnetChecks();

    GeoLookupUtil geoLookupUtil = mock(GeoLookupUtil.class);
    when(geoLookupUtil.lookupCountry(anyString())).thenReturn("BE");
    when(geoLookupUtil.lookupCountry(any(InetAddress.class) )).thenReturn("BE");

    DNSParquetPacketWriter writer = new DNSParquetPacketWriter(checks, geoLookupUtil);
    Path dir = Files.createTempDirectory("DNSParquetPacketWriterTest");

    System.out.println();
    System.out.println("dir = " + dir);
    System.out.println();

    writer.open(dir.toString());

    Packet request = new DNSPacket();
    Message requestMessage = new Message();
    ServerInfo serverInfo = new ServerInfo("test.example.com");

    DateTime dateTime = new DateTime();
    dateTime.getYear();

    request.setSrc("10.20.21.33");
    request.setSrcPort(5301);
    request.setDst("8.7.6.5");
    request.setDstPort(53);
    request.setFragmented(false);
    request.setFragOffset(1005);
    request.setIpHeaderLen(500);
    request.setIpId(154566L);
    request.setIpVersion( (byte)IPDecoder.IP_PROTOCOL_VERSION_4);
    request.setLen(406);
    request.setPayloadLength(208);
    request.setTs(1551552698L);
    request.setUdpLength(508);
    request.setTtl(209);

    Header header = new Header();
    header.setOpCode(OpcodeType.STANDARD);

    requestMessage.setBytes(506);
    requestMessage.addHeader(header);
    requestMessage.addQuestion(new Question("abc.example.com", ResourceRecordType.A, ResourceRecordClass.IN));

    PacketCombination combination = new PacketCombination(request, requestMessage, serverInfo, true, "test.pcap");
    writer.write(combination);
    writer.close();
    FileHelper fileHelper = new FileHelper();

    List<File> files = fileHelper.findRecursively(dir.toFile(), "parquet");
    logger.info("files = {}", files);
    assertTrue("at least one parquet file should be created", files.size() > 0);
    assertTrue(files.get(0).getAbsolutePath().contains("year=2019/month=03/day=02/server=test.example.com"));

  }

}