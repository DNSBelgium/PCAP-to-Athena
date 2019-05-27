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
