package parquet.org.slf4j.impl;

import org.slf4j.impl.Log4jLoggerFactory;
import parquet.org.slf4j.helpers.NOPLoggerFactory;

/**
 * Because of a bug in parquet-format ( https://issues.apache.org/jira/browse/PARQUET-369 )
 * an annoying warning is emitted when writin parquet files.
 *
 * The shaded slf4j tries to load parquet.org.slf4j.impl.StaticLoggerBinder which does not exist
 *
 * Therefor we add this class ourselves. It does not matter what the class does, just that it exists.
 *
 * Sigh, java & logging ...
 *
 */
@SuppressWarnings("unused")
public class StaticLoggerBinder {

  private static final parquet.org.slf4j.impl.StaticLoggerBinder SINGLETON = new StaticLoggerBinder();

  public parquet.org.slf4j.ILoggerFactory getLoggerFactory() {
    return new NOPLoggerFactory();
  }

  public String getLoggerFactoryClassStr() {
    return Log4jLoggerFactory.class.getName();
  }

  /**
   * Return the singleton of this class.
   *
   * @return the StaticLoggerBinder singleton
   */
  @SuppressWarnings("FinalStaticMethod")
  public static final parquet.org.slf4j.impl.StaticLoggerBinder getSingleton() {
    return SINGLETON;
  }

  private StaticLoggerBinder() {
  }


}
