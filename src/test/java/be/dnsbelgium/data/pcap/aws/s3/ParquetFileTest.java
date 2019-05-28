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

package be.dnsbelgium.data.pcap.aws.s3;

import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

public class ParquetFileTest {

  private static final Logger logger = getLogger(ParquetFileTest.class);


  @Test
  // TODO quentinl Isn't it weird that baseFolder is not a parent of file ?
  public void testConstructor() {
    File baseFolder = new File("/anything/can/go/here/parquet/e621a356/");
    File file =  new File("/anything/can/go/here/before/e621a356/year=2018/month=10/day=17/server=prague1/and-after/as-well-134545.parquet");
    ParquetFile parquetFile = new ParquetFile(baseFolder, file);
    logger.info("parquetFile = {}", parquetFile);
    assertNotNull("parquetFile should have a date", parquetFile.getDate());
    assertNotNull("parquetFile should have a server", parquetFile.getServer());
    assertEquals("2018-10-17", parquetFile.getDate().toString());
    assertEquals("prague1", parquetFile.getServer());
  }

  @Test
  public void wrongFolderStructure() {
    File baseFolder = new File("/data/pcap-to-athena/parquet/e621a356/");
    File file =  new File("/data/pcap-to-athena/parquet/e621a356/yea=2018/month=10/day=17/server=prague1/bc12dcd0-134545.parquet");
    ParquetFile parquetFile = new ParquetFile(baseFolder, file);
    logger.info("parquetFile = {}", parquetFile);
    assertNull("parquetFile should have NOT a date", parquetFile.getDate());
    assertNull("parquetFile should have NOT a server", parquetFile.getServer());

  }

}