package be.dnsbelgium.data.pcap.handler;

import au.com.bytecode.opencsv.CSVWriter;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.types.MessageType;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.support.MessageWrapper;
import nl.sidn.pcap.support.RequestKey;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

public class PcapAnalyzer implements PacketHandler {

  private long packetCount = 0;
  private long oldest = Integer.MAX_VALUE;
  private long newest = Integer.MIN_VALUE;
  Map<Integer, Integer> protoCount = new HashMap<>();
  private int tcpCount = 0;
  private int udpCount = 0;
  private long queryCount = 0;
  private long responseCount = 0;
  private long unmatchedResponses = 0;
  private long matchedResponses = 0;
  private Set<String> qNames = new HashSet<>();
  private Map<RequestKey, MessageWrapper> requests  = new HashMap<>();
  private Map<RequestKey, MessageWrapper> responses = new HashMap<>();
  private String fileName = "";
  private static final Logger logger = getLogger(PcapAnalyzer.class);


  @Override
  public boolean handle(Packet packet) {

    packetCount++;
    int proto = packet.getProtocol();
    if (proto == 17) {
      udpCount++;
    }
    if (proto == 6) {
      tcpCount++;
    }
    Integer count = protoCount.get(proto);
    if (count == null) {
      count = 0;
    }
    protoCount.put(proto, count + 1);

    DNSPacket dnsPacket = (DNSPacket) packet;


    for (Message msg : dnsPacket.getMessages()) {
      // get qname from request which is part of the cache lookup key
      String qname = null;
      if (msg != null && msg.getQuestions() != null && msg.getQuestions().size() > 0) {
        qname = msg.getQuestions().get(0).getqName();
        qNames.add(qname);
      }
      if (msg != null && msg.getHeader().getQr() == MessageType.QUERY) {
        queryCount++;
        RequestKey key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getSrc(), dnsPacket.getSrcPort(), System.currentTimeMillis());
        requests.put(key, new MessageWrapper(msg, dnsPacket, fileName));

        if (qname == null) {
          logger.info("Query without qname: msg = {}", msg);
        }

      } else {
        responseCount++;
        assert msg != null;
        RequestKey key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getDst(), dnsPacket.getDstPort());

        if (qname == null) {
          logger.info("Counter={} => Response without qname: msg = {}", packetCount, msg);
          logger.info("Counter={} => Response without qname: msg = {}", packetCount, dnsPacket);
        }

        responses.put(key, new MessageWrapper(msg, dnsPacket, fileName));
        MessageWrapper request = requests.get(key);
        if (request == null) {
          unmatchedResponses++;
        } else {
          matchedResponses++;
        }
      }
    }
    oldest = Math.min(oldest, dnsPacket.getTs());
    newest = Math.max(newest, dnsPacket.getTs());

    return true;
  }

  public void logAnalysis() throws IOException {
    logger.info("qNames.size = {}", qNames.size());
    logger.info("packetCount        = {}", packetCount);
    logger.info("queryCount         = {}", queryCount);
    logger.info("responseCount      = {}", responseCount);
    logger.info("responses for which we found a matching request        : {}", matchedResponses);
    logger.info("responses for which we did not find a matching request : {}", unmatchedResponses);

    logger.info("requests.keySet().size()  = {}", requests.keySet().size());
    logger.info("responses.keySet().size() = {}", responses.keySet().size());


    // id=8584, qname=dogtrack.be., src=173.194.170.108, srcPort=56714, time=0]

    RequestKey key = new RequestKey(8584, "dogtrack.be.", "173.194.170.108", 56714);

//    MessageWrapper request = requests.get(key);
//    logger.info("request for dogtrack: {}", request.getMessage());
//
//    MessageWrapper response = responses.get(key);
//    logger.info("response for dogtrack = {}", response.getMessage());

    Set<RequestKey> requestKeys = new HashSet<>(requests.keySet());
    logger.info("requestKeys = {}", requestKeys.size());

    logger.info("before remove: requestKeys.contains(dogtrack) : {}", requestKeys.contains(key));
    logger.info("before remove: responses.keySet().contains(dogtrack) : {}", responses.keySet().contains(key));

    requestKeys.removeAll(responses.keySet());

    logger.info("after remove: requestKeys.contains(dogtrack) : {}", requestKeys.contains(key));

    logger.info("unmatched requestKeys = {}", requestKeys.size());

    Set<RequestKey> responseKeys = new HashSet<>(responses.keySet());

    logger.info("before remove: responseKeys.contains(dogtrack) : {}", responseKeys.contains(key));
    logger.info("before remove: requests.keySet().contains(dogtrack) : {}", requests.keySet().contains(key));

    logger.info("responseKeys = {}", responseKeys.size());

    responseKeys.removeAll(requests.keySet());
    logger.info("after remove: responseKeys.keySet().contains(dogtrack) : {}", responseKeys.contains(key));

    logger.info("unmatched responseKeys = {}", responseKeys.size());

    DateTime tsOldest = new DateTime(oldest * 1000L);
    DateTime tsNewest = new DateTime(newest * 1000L);
    logger.info("tsOldest = {}", tsOldest);
    logger.info("tsNewest = {}", tsNewest);

    Duration duration = new Duration(tsOldest, tsNewest);
    logger.info("duration = {}", duration);
    logger.info("duration = {} seconds", duration.getStandardSeconds());
    logger.info("duration = {} seconds", duration.getStandardSeconds());

    logger.info("udpCount = {}", udpCount);
    logger.info("tcpCount = {}", tcpCount);
    logger.info("protoCount = {}", protoCount);

    CSVWriter responseWriter = new CSVWriter(new FileWriter("unmatchedResponses.csv"));
    CSVWriter requestWriter  = new CSVWriter(new FileWriter("unmatchedRequests.csv"));

    for (RequestKey responseKey : responseKeys) {
      //logger.info("No request found for {}", responseKey);
      responseWriter.writeNext(new String[]{ responseKey.toString() });
    }
    responseWriter.flush();

    for (RequestKey requestKey : requestKeys) {
      //logger.info("No response found for {}", requestKey);
      requestWriter.writeNext(new String[]{ requestKey.toString() });
    }
    requestWriter.flush();


  }
}
