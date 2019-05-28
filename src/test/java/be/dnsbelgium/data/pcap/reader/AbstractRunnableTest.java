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

import org.junit.Test;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

public class AbstractRunnableTest {

  private static final Logger logger = getLogger(AbstractRunnableTest.class);

  @Test
  public void testRun() {
    AtomicBoolean ok = new AtomicBoolean(false);
    AbstractRunnable runnable = new AbstractRunnable() {
      @Override
      protected void doRun() {
        ok.set(true);
      }
    };
    runnable.doRun();
    assertTrue(ok.get());
  }

  @Test
  public void whenRunThrows_ExceptionIsLogged() throws InterruptedException {
    logger.info("we should see the exception");
    AbstractRunnable runnable = new AbstractRunnable() {
      @Override
      protected void doRun() {
        throw new IllegalArgumentException("test");
      }
    };
    ExecutorService executor = Executors.newFixedThreadPool(1);
    executor.submit(runnable);
    executor.shutdown();
    executor.awaitTermination(100, TimeUnit.MILLISECONDS);
  }

}