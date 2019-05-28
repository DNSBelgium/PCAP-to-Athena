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

package be.dnsbelgium.data.pcap.ip;

import com.google.common.net.InetAddresses;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.slf4j.LoggerFactory.getLogger;

public class SubnetCheck {

  private Set<Subnet> bit_subnets = new HashSet<>();
  private Set<String> subnets = new HashSet<>();
  private File file;
  private SubnetFetcher subnetFetcher;
  private Map<String, Boolean> matchCache = new HashMap<>();
  private boolean initialized = false;

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private final static int maxAgeInHours = 24;

  private static final Logger logger = getLogger(SubnetCheck.class);

  public SubnetCheck(File file, SubnetFetcher subnetFetcher) {
    this.file = file;
    this.subnetFetcher = subnetFetcher;
  }

  /**
   * Try to read resolvers from file.  Then calls update() to checks if data needs to be fetched again
   * Will also fetch the data when reading from file fails
   */
  public void init() {
    if (initialized) {
      logger.warn("You should call init() only once !!");
      return;
    }
    if (loadFromFile()) {
      initialized = true;
      logger.info("Loaded {} resolvers from file", getSubnetCount());
      update();
    } else {
      logger.info("Fetching resolvers from original source");
      fetchDataAndSaveToFile();
      initialized = true;
    }
  }

  /**
   * Will check lastModified of file and fetch external data when file is older than <code>maxAgeInHours</code>
   * or when the file does not exist.
   */
  public void update() {
    if (!initialized) {
      loadFromFile();
    }
    if (file.exists()) {
      DateTime lastModifiedDate = new DateTime(file.lastModified());
      DateTime expiryDate = DateTime.now().minusHours(maxAgeInHours);
      if (lastModifiedDate.isBefore(expiryDate)) {
        logger.info("Resolver file {} is too old: {} => fetching external data now.", file, lastModifiedDate);
        fetchDataAndSaveToFile();
      } else {
        logger.info("File {} is recent: {} => no need to fetch data again", file, lastModifiedDate);
      }
    } else {
      logger.debug("Resolver file {} does not exist, fetching now.", file);
      fetchDataAndSaveToFile();
    }
  }

  private void fetchDataAndSaveToFile(){
    try {
      List<String> ranges = subnetFetcher.fetchSubnets();
      if (ranges.size() > 0) {
        logger.info("fetched {} subnets from external source => updating data in-memory and on-disk", ranges.size());
        replaceDataWith(ranges);
        writeToFile();
      }
    } catch (IOException e) {
      logger.error("Error while fetching resolver addresses. Not saving to file", e);
    }
  }

  private void replaceDataWith(List<String> ranges) {
    if (ranges.isEmpty()) {
      logger.info("new data is empty => not updating");
      return;
    }
    readWriteLock.writeLock().lock();
    try {
      matchCache.clear();
      subnets.clear();
      bit_subnets.clear();
      for (String range : ranges) {
        add(range);
      }
      logger.info("refreshed data with {} subnets", subnets.size());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void add(String range) {
    try {
      bit_subnets.add(Subnet.createInstance(range));
      subnets.add(range);
    } catch (UnknownHostException e) {
      logger.error("UnknownHostException: skipping range {}", range);
    }
  }

  /**
   * Load subnets from file
   * @return true if loading from file was succesful
   */
  private boolean loadFromFile() {
    if (file.exists()) {
      try {
        return readFromFile(file);
      } catch (IOException e) {
        logger.warn("Failed to read file " + file, e);
        return false;
      }
    }
    logger.warn("File {} does not exist", file);
    return false;
  }

  private boolean readFromFile(File file) throws IOException {
    logger.info("Loading resolver addresses from file: " + file);
    List<String> lines = Files.readAllLines(file.toPath());
    logger.info("Loaded {} subnets from {}", lines.size(), file.getAbsolutePath());
    replaceDataWith(lines);
    return getSubnetCount() > 0;
  }

  private void writeToFile() {
    logger.info("writing {} subnets to {}", getSubnetCount(), file.getAbsolutePath());
    readWriteLock.readLock().lock();
    try {
      Files.write(file.toPath(), subnets, CREATE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      logger.error("Failed to save subnets to " + file.getAbsolutePath(), e);
    } finally {
      readWriteLock.readLock().unlock();
    }
    logger.info("Finished writing subnets to {}", file);
  }

  public boolean isMatch(String address) {
    readWriteLock.readLock().lock();
    try {
      return _isMatch(address);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getSubnetCount() {
    readWriteLock.readLock().lock();
    try {
      return subnets.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private boolean _isMatch(String address){
    Boolean cacheHit = matchCache.get(address);
    if(cacheHit != null){
      return cacheHit;
    }
    InetAddress ipAddress = InetAddresses.forString(address);
    boolean match = bitCompare(ipAddress);

    //create cache with hashmap for matches for perf
    matchCache.put(address,match);

    return match;
  }

  private boolean bitCompare(InetAddress ipAddress) {
    readWriteLock.readLock().lock();
    try {
      for (Subnet sn : bit_subnets) {
        if (sn.isInNet(ipAddress)) {
          return true;
        }
      }
      return false;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public File getFile() {
    return file;
  }
}
