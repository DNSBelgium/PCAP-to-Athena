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
