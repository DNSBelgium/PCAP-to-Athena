package be.dnsbelgium.data.pcap.aws.athena;

import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.slf4j.LoggerFactory.getLogger;

public class TestExceptions {

  private static final Logger logger = getLogger(TestExceptions.class);

  @Test
  public void exceptions() {
    String ATHENA_DS = "ATHENA_DS";
    ATHENA_DS = "com.simba.athena.jdbc42.DataSourcexx";
    try {
      Class<?> clazz = Class.forName(ATHENA_DS);
      Constructor<?> ctor = clazz.getConstructor();
      Object object = ctor.newInstance();
      Method method = clazz.getMethod("setCustomProperty", String.class, String.class);
      method.invoke(object, "LogLevel", "4");
    } catch (ClassNotFoundException|NoSuchMethodException|InstantiationException|IllegalAccessException|InvocationTargetException e) {
      logger.warn("Failed to set loglevel of {}. Exception={} Message={}", ATHENA_DS, e.getClass().getName(), e.getMessage());
    }
  }

}
