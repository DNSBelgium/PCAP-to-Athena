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

package be.dnsbelgium.data.pcap.parquet;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import parquet.Log;

import javax.annotation.PostConstruct;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class ParquetLogging {

  private static final Logger logger = getLogger(ParquetLogging.class);

  @PostConstruct
  public static void removeHandlers() {
    // Note: the parquet.Log class has a static initializer that
    // sets the java.util.logging Logger for "parquet". This
    // checks first to see if there's any handlers already set
    // and if not it creates them. If this method executes prior
    // to that class being loaded then:
    //  1) there's no handlers installed so there's none to
    // remove. But when it IS finally loaded the desired affect
    // of removing them is circumvented.
    //  2) The parquet.Log static initializer calls setUseParentHanders(false)
    // undoing the attempt to override the logging here.
    //
    // Therefore we need to force the class to be loaded.
    // This should really be resolved by Parquet.

    // access class parquet.Log => trigger static initialization block where the handler is created and added

    java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger("parquet");

    try {
      Class.forName("parquet.Log").getName();
    } catch (Exception ignore) {
    }

    // Note: Logger.getLogger("parquet") has a hard-coded handler that appends to Console. Let's remove this.

    logger.info("Removing hard-coded java.util.logging.Handler");

    // now remove that handler
    Handler[] handlers = julLogger.getHandlers();
    logger.info("java.util.logging.Logger of {} has {} handlers", julLogger.getName(), handlers.length);
    for (Handler handler : julLogger.getHandlers()) {
      logger.info("Removing handler {}", handler.getClass());
      julLogger.removeHandler(handler);
    }
    julLogger.setUseParentHandlers(false);
    julLogger.setLevel(Level.OFF);
    Log.getLog(parquet.hadoop.ParquetFileWriter.class).debug("test");

    julLogger.addHandler(new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel().intValue() > Level.INFO.intValue()) {
          logger.warn(record.getMessage());
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() throws SecurityException {
      }
    });

  }

}
