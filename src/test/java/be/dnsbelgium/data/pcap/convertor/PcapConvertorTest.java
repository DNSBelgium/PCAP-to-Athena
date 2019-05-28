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

import be.dnsbelgium.data.pcap.ip.SubnetChecks;
import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.parquet.ParquetLogging;
import be.dnsbelgium.data.pcap.reader.PcapReaderConfig;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import be.dnsbelgium.data.pcap.utils.GeoLookupUtil;
import nl.sidn.dnslib.types.ResourceRecordType;
import org.apache.commons.io.FileUtils;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import parquet.Log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.slf4j.LoggerFactory.getLogger;

public class PcapConvertorTest {

  private GeoLookupUtil geoLookupUtil = mock(GeoLookupUtil.class);
  private SubnetChecks subnetChecks = new SubnetChecks();
  private FileHelper fileHelper = mock(FileHelper.class);
  private PcapReaderConfig config;

  private static final org.slf4j.Logger logger = getLogger(PcapConvertorTest.class);
  private List<File> tempFiles = new ArrayList<>();

  @After
  public void after() {
    for (File file : tempFiles) {
      if (!file.delete()) {
        file.deleteOnExit();
      }
      if (file.isDirectory()) {
        FileUtils.deleteQuietly(file);
      }
    }
  }

  @Test
  public void convert() throws InterruptedException, IOException {
    // for some reason these 3 statements have to be here and don't do their job when in an @Before method
    ParquetLogging.removeHandlers();
    parquet.Log log = Log.getLog(parquet.hadoop.ParquetFileWriter.class);
    log.debug("this is debug");


    // use empty decoderStateFolder to avoid reading old decoder state
    File stateFolder = Files.createTempDirectory("PcapConvertorTest.state").toFile();
    // time-out needs to be high enough, otherwise asserts below will fail
    int timeOutInMs = 10000;
    config = new PcapReaderConfig(timeOutInMs, timeOutInMs, timeOutInMs, 48000,
        stateFolder.getAbsolutePath(), 1000);

    PcapConvertor convertor = new PcapConvertor(geoLookupUtil, subnetChecks, config, fileHelper);

    ClassPathResource resource = new ClassPathResource("pcap/dns3.pcap");
    File pcapFile = resource.getFile();
    ServerInfo serverInfo = new ServerInfo("ns1.dns.be");
    File outputFolder = Files.createTempDirectory("PcapConvertorTest").toFile();
    tempFiles.add(outputFolder);

    logger.info("outputFolder = {}", outputFolder);

    LocalConversionJob localConversionJob = new LocalConversionJob(serverInfo, Lists.newArrayList(pcapFile), outputFolder);
    Metrics metrics = convertor.convertToParquet(localConversionJob);

    FileHelper fileHelper = new FileHelper();
    List<File> files = fileHelper.findRecursively(outputFolder, "parquet");
    files.sort(Comparator.comparing(File::getAbsolutePath));

    for (File file : files) {
      logger.info("file = {}", file);
      tempFiles.add(file);
    }
    assertEquals(2, files.size());

    String file1 = files.get(0).getAbsolutePath().replace(outputFolder.getAbsolutePath(), "");
    String file2 = files.get(1).getAbsolutePath().replace(outputFolder.getAbsolutePath(), "");

    logger.info("file1 = {}", file1);
    logger.info("file2 = {}", file2);

    assertTrue(file1.startsWith("/year=2005/month=03/day=30/server=ns1.dns.be/"));
    assertTrue(file2.startsWith("/year=2005/month=03/day=31/server=ns1.dns.be/"));

    assertTrue(file1.endsWith(".parquet"));
    assertTrue(file2.endsWith(".parquet"));

    assertEquals("Oldest packet","2005-03-30T23:59:38Z", metrics.getOldestPacket().toString());
    assertEquals("Newest packet","2005-03-31T00:04:17Z", metrics.getNewestPacket().toString());

    logger.info("metrics = {}", metrics);

    for (int qtype : metrics.getQtypes().keySet()) {
      ResourceRecordType type = ResourceRecordType.fromValue(qtype);
      logger.info("{} : {} => {}", qtype, type, metrics.getQtypes().get(qtype));
    }

    logger.info("ipv4QueryCount = {}", metrics.getIpv4QueryCount());
    logger.info("ipv6QueryCount = {}", metrics.getIpv6QueryCount());


    assertEquals("responseBytes", 2132, metrics.getResponseBytes());
    assertEquals("requestBytes",  1574, metrics.getRequestBytes());

    assertEquals("ipv4QueryCount",  19, metrics.getIpv4QueryCount());
    assertEquals("ipv6QueryCount",  0, metrics.getIpv6QueryCount());

    assertEquals("rcode=0", 13, metrics.getRcodes().get(0).intValue());
    assertEquals("rcode=3", 6,  metrics.getRcodes().get(3).intValue());

    assertEquals("A queries"   , 3, metrics.getQtypes().get(ResourceRecordType.A.getValue()).intValue() );
    assertEquals("AAAA queries", 6, metrics.getQtypes().get(ResourceRecordType.AAAA.getValue()).intValue() );
    assertEquals("MX queries",   1,  metrics.getQtypes().get(ResourceRecordType.MX.getValue()).intValue() );
    assertEquals("ANY queries",   1,  metrics.getQtypes().get(ResourceRecordType.ANY.getValue()).intValue() );
    assertEquals("TXT queries",   1,  metrics.getQtypes().get(ResourceRecordType.TXT.getValue()).intValue() );
    assertEquals("NS queries",   1,  metrics.getQtypes().get(ResourceRecordType.NS.getValue()).intValue() );
    assertEquals("SRV queries",   3,  metrics.getQtypes().get(ResourceRecordType.SRV.getValue()).intValue() );
    assertEquals("LOC queries",   1,  metrics.getQtypes().get(ResourceRecordType.LOC.getValue()).intValue() );

    assertEquals("fragmented UDP requests",  0, metrics.getRequestUDPFragmentedCount());
    assertEquals("fragmented UDP responses", 0, metrics.getResponseUDPFragmentedCount());
    assertEquals("fragmented TCP requests",  0, metrics.getRequestTCPFragmentedCount());
    assertEquals("fragmented TCP responses", 0, metrics.getResponseTCPFragmentedCount());

    assertEquals("Opcode 0", 19, metrics.getOpcodes().get(0).intValue());
    assertEquals("Expired DNS queries", 0, metrics.getExpiredDnsQueryCount());

    logger.info("is_google: {}", metrics.getMatchCount("is_google"));

    // No asserts on metrics.getMatchCount("is_google")
    // since these metrics are updated by DNSParquetPacketWriter who is not invoked in this test

  }


}