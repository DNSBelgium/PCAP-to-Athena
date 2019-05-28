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
