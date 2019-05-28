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

package be.dnsbelgium.data.pcap.reader;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Simple Runnable with Logging. Catch any exception and transform it as unchecked exception.
 */
public abstract class AbstractRunnable implements Runnable {

  private static final Logger logger = getLogger(AbstractRunnable.class);

  abstract protected void doRun();

  @Override
  public void run() {
    try {
      logger.info("Runnable {} starting ...", getClass());
      doRun();
      logger.info("Runnable {} is done", getClass());
    } catch (Exception e) {
      logger.error("Thread threw exception: " + Thread.currentThread(), e);
      throw new RuntimeException(e);
    }
  }
}
