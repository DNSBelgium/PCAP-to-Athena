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

package be.dnsbelgium.data.pcap.utils;

import java.util.Locale;

public class FileSize {

  public final static long BYTES_PER_GB = (1024L*1024*1024);

  public static String friendlySize(long bytes) {
    if (bytes < 1024) {
      return String.format(Locale.US, "%s bytes", bytes);
    }
    if (bytes < 1024 * 1024) {
      return String.format(Locale.US, "%s Kilobytes", bytes/1024);
    }
    if (bytes < 1024L * 1024L * 1024L) {
      return String.format(Locale.US, "%s Megabytes", bytes/(1024*1024));
    }
    if (bytes < 1024L * 1024L * 1024L * 1024L) {
      return String.format(Locale.US, "%.3f Gigabytes", bytes/(1024L*1024*1024.0));
    }
    if (bytes < 1024L * 1024L * 1024L * 1024L * 1024L) {
      return String.format(Locale.US, "%.3f Terabytes", bytes/(1024L*1024*1024*1024.0));
    }
    return String.format(Locale.US, "%.3f Petabytes", bytes/(1024L*1024*1024*1024*1024.0));
  }

  public static String friendlyThroughput(long bytes, long millis) {
    double bits = 8L * bytes;
    double seconds = millis / 1000.0;
    double tp = bits/seconds;
    if (tp < 1024) {
      return String.format(Locale.US, "%2.1f bit/s", tp);
    }
    tp = tp / 1000.0;
    if (tp < 1000) {
      return String.format(Locale.US, "%2.1f Kbps", tp);
    }
    tp = tp / 1000;
    if (tp < 1000) {
      return String.format(Locale.US, "%2.2f Mbps", tp);
    }
    tp = tp / 1000;
    if (tp < 1000) {
      return String.format(Locale.US, "%2.2f Gbps", tp);
    }
    return String.format(Locale.US, "%2.2f Pbps", tp/1000);

  }

}
