package be.dnsbelgium.data.pcap.convertor;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {

  private long responseBytes = 0;
  private long requestBytes = 0;
  private Map<Integer, Integer> qtypes = new HashMap<>();
  private Map<Integer, Integer> rcodes = new HashMap<>();
  private Map<Integer, Integer> opcodes = new HashMap<>();
  private Map<String, AtomicInteger> subnetMatchCount = new HashMap<>();

  private int requestUDPFragmentedCount = 0;
  private int requestTCPFragmentedCount = 0;
  private int responseUDPFragmentedCount = 0;
  private int responseTCPFragmentedCount = 0;
  private int ipv4QueryCount = 0;
  private int ipv6QueryCount = 0;

  private int expiredDnsQueryCount = 0;

  private Instant oldestPacket = Instant.MAX;
  private Instant newestPacket = Instant.MIN;

  public void incrementQueryType(int qtype) {
    updateMetricMap(qtypes, qtype);
  }

  public void incrementRcode(int rcode) {
    updateMetricMap(rcodes, rcode);
  }

  public void incrementOpcode(int opcode) {
    updateMetricMap(opcodes, opcode);
  }

  private void updateMetricMap(Map<Integer, Integer> map, Integer key) {
    map.merge(key, 1, Integer::sum);
  }

  public void incrementExpiredDnsQueryCount() {
    expiredDnsQueryCount++;
  }

  public void incrementResponseBytes(int bytes) {
    responseBytes = responseBytes + bytes;
  }

  public void incrementRequestBytes(int bytes) {
    requestBytes = requestBytes + bytes;
  }


  public long getResponseBytes() {
    return responseBytes;
  }

  public long getRequestBytes() {
    return requestBytes;
  }

  public Map<Integer, Integer> getQtypes() {
    return qtypes;
  }

  public Map<Integer, Integer> getRcodes() {
    return rcodes;
  }

  public Map<Integer, Integer> getOpcodes() {
    return opcodes;
  }

  public int getRequestUDPFragmentedCount() {
    return requestUDPFragmentedCount;
  }

  public int getRequestTCPFragmentedCount() {
    return requestTCPFragmentedCount;
  }

  public int getResponseUDPFragmentedCount() {
    return responseUDPFragmentedCount;
  }

  public int getResponseTCPFragmentedCount() {
    return responseTCPFragmentedCount;
  }

  public int getIpv4QueryCount() {
    return ipv4QueryCount;
  }

  public int getIpv6QueryCount() {
    return ipv6QueryCount;
  }

  public void incrementRequestUDPFragmentedCount() {
    requestUDPFragmentedCount++;
  }

  public void incrementRequestTCPFragmentedCount() {
    requestTCPFragmentedCount++;
  }

  public void incrementResponseUDPFragmentedCount() {
    responseUDPFragmentedCount++;
  }

  public void incrementResponseTCPFragmentedCount() {
    responseTCPFragmentedCount++;
  }

  public void incrementIpv4QueryCount() {
    ipv4QueryCount++;
  }

  public void incrementIpv6QueryCount() {
    ipv6QueryCount++;
  }

  public int getExpiredDnsQueryCount() {
    return expiredDnsQueryCount;
  }

  public void registerTimestamp(Instant instant) {
    if (instant.isAfter(newestPacket)) {
      newestPacket = instant;
    }
    if (instant.isBefore(oldestPacket)) {
      oldestPacket = instant;
    }
  }

  public Instant getOldestPacket() {
    return oldestPacket;
  }

  public Instant getNewestPacket() {
    return newestPacket;
  }

  @Override
  public String toString() {
    return "Metrics{" +
        "responseBytes=" + responseBytes +
        ", requestBytes=" + requestBytes +
        ", qtypes=" + qtypes +
        ", rcodes=" + rcodes +
        ", opcodes=" + opcodes +
        ", subnetMatchCount=" + subnetMatchCount +
        ", requestUDPFragmentedCount=" + requestUDPFragmentedCount +
        ", requestTCPFragmentedCount=" + requestTCPFragmentedCount +
        ", responseUDPFragmentedCount=" + responseUDPFragmentedCount +
        ", responseTCPFragmentedCount=" + responseTCPFragmentedCount +
        ", ipv4QueryCount=" + ipv4QueryCount +
        ", ipv6QueryCount=" + ipv6QueryCount +
        ", expiredDnsQueryCount=" + expiredDnsQueryCount +
        ", oldestPacket=" + oldestPacket +
        ", newestPacket=" + newestPacket +
        '}';
  }

  public void incrementSubnetMatch(String key) {
    AtomicInteger counter = subnetMatchCount.get(key);
    if (counter == null) {
      counter = new AtomicInteger();
      subnetMatchCount.put(key, counter);
    }
    counter.incrementAndGet();
  }

  public Map<String, Integer> getSubnetMatchCount() {
    Map<String, Integer> counters = new HashMap<>();
    for (String s : subnetMatchCount.keySet()) {
      counters.put(s, subnetMatchCount.get(s).intValue());
    }
    return Collections.unmodifiableMap(counters);
  }

  public int getMatchCount(String key) {
    AtomicInteger count = subnetMatchCount.get(key);
    if (count == null) {
      return 0;
    }
    return count.intValue();
  }

}
