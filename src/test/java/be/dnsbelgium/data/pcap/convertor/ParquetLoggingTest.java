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

import be.dnsbelgium.data.pcap.parquet.ParquetLogging;
import org.junit.Test;
import parquet.Log;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class ParquetLoggingTest {


  // when you run only this test method, you will see info, warn and error messages
  // from parquet.hadoop.ParquetFileWriter (via java.util.logging)
  @Test
  public void logging() {
    parquet.Log log = Log.getLog(parquet.hadoop.ParquetFileWriter.class);
    log.debug("this is debug");
    log.info ("this is info");
    log.warn ("this is warn");
    log.error("This is error");
    Logger.getLogger("").warning("A TEST warning from root logger. Please ignore.");
  }

  @Test
  // when you run this test method, you should see NO messages from parquet.hadoop.ParquetFileWriter (via java.util.logging)
  public void disableLogging() {
    ParquetLogging.removeHandlers();
    parquet.Log log = Log.getLog(parquet.hadoop.ParquetFileWriter.class);
    log.debug("this is debug");
    log.info ("this is info");
    log.warn ("this is warn");
    log.error("This is error");
    Logger.getLogger("").warning("A TEST warning from root logger. Please ignore.");

    java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger("parquet");
    for (Handler handler : julLogger.getHandlers()) {
      handler.publish(new LogRecord(Level.INFO, "an INFO message"));
      handler.publish(new LogRecord(Level.SEVERE, "a SEVERE message"));
    }

  }
}
