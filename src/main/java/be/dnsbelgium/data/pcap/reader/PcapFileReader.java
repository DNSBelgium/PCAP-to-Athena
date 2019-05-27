package be.dnsbelgium.data.pcap.reader;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import be.dnsbelgium.data.pcap.utils.FileSize;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.types.MessageType;
import nl.sidn.dnslib.types.ResourceRecordType;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.SequencePayload;
import nl.sidn.pcap.decoder.ICMPDecoder;
import nl.sidn.pcap.packet.*;
import nl.sidn.pcap.support.MessageWrapper;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.support.RequestKey;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.slf4j.LoggerFactory.getLogger;

/*
 * This class is heavily based on
 *  https://github.com/SIDN/entrada/blob/master/pcap-to-parquet/src/main/java/nl/sidn/pcap/load/LoaderThread.java
 *
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 * This product includes code from ENTRADA created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
 * <a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
 *
 */

public class PcapFileReader extends AbstractRunnable {

  private static final Logger logger = getLogger(PcapFileReader.class);

  private static final int LOG_FREQUENCY = 100000;
  private static final String DECODER_STATE_FILE = "pcap-decoder-state";

  private final List<File> inputFiles;
  private final PcapReaderConfig config;
  private final BlockingQueue<PacketCombination> sharedQueue;
  private final ServerInfo serverInfo;

  private Map<RequestKey, Integer> activeZoneTransfers = new HashMap<>();
  private Map<RequestKey, MessageWrapper> _requestCache = new HashMap<>();

  private int timeOuts = 0;
  private int malformed = 0;
  private int added = 0;

  private int multiCounter = 0;
  private int queryCounter = 0;
  private int responseCounter = 0;
  // counter when no request query can be found for a response
  private int noQueryFoundCounter = 0;
  private int purgeCounter = 0;
  private final PcapReader pcapReader;

  public PcapFileReader(PcapReaderConfig config, ServerInfo serverInfo, List<File> inputFiles, BlockingQueue<PacketCombination> sharedQueue) {
    this.config = config;
    this.serverInfo = serverInfo;
    this.sharedQueue = sharedQueue;
    this.pcapReader = new PcapReader();
    this.inputFiles = inputFiles;
  }

  @Override
  protected void doRun() {
    logger.info("Starting to read {} files of {}", inputFiles.size(), serverInfo);

    long bytesTotal = inputFiles.stream().mapToLong(File::length).sum();
    long bytesProcessed = 0;
    int filesTotal = inputFiles.size();
    int filesProcessed = 0;

    loadState();
    String fileName = "";
    try {
      for (File inputFile : inputFiles) {
        fileName = inputFile.getAbsolutePath();
        logger.info("Starting to read {} of {}", inputFile, serverInfo);
        read(inputFile.getAbsolutePath());
        purgeCache();
        bytesProcessed += inputFile.length();
        filesProcessed++;
        logger.info("Processed {} of {} files:  {} of {}",
            filesProcessed, filesTotal, FileSize.friendlySize(bytesProcessed), FileSize.friendlySize(bytesTotal));
      }
    } catch (Exception e) {
      logger.error("Failed to read file [{}] {}:{}", fileName, e.getClass(), e.getMessage());
      // TODO: Add PacketCombination.FAILURE in case we get an exception
//      addLastPacket(PacketCombination.FAILURE);
    }

    // add marker packet indicating all packets are decoded
    // this will cause the controller thread to stop all processing.
    addLastPacket(PacketCombination.NULL);
    // save unmatched packet state to file,  the next pcap might have the missing responses
    persistState();
    logMetrics();
  }

  private void addLastPacket(PacketCombination combination) {
    boolean added = false;
    int attempts = 0;
    while (!added) {
      try {
        attempts++;
        logger.info("Trying to add last combination to the queue. attempts={} size={}", attempts, sharedQueue.size());
        added = sharedQueue.offer(combination, 30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while adding last packet => retry until success since we need to signal the end to other threads");
      }
    }
    logger.info("Could add last packet after {} attempts", attempts);
  }

  protected void logMetrics() {
    logger.info("---------------------------------------------------------------");
    logger.info("Finished reading {} PCAP files of {}", inputFiles.size(), serverInfo);
    logger.info("Found " + (queryCounter + responseCounter) + " packets");
    logger.info("Found " + queryCounter + " query packets");
    logger.info("Found " + responseCounter + " response packets");
    logger.info("Found " + multiCounter + " messages from TCP streams with > 1 mesg");
    logger.info("Found " + noQueryFoundCounter + " response packets without request.");
    logger.info("Timeouts: {}", timeOuts);
    logger.info("Malformed: {}", malformed);
    logger.info("Combinations added : {}", added);
    logger.info("request.cache : {}", _requestCache.size());
    logger.info("---------------------------------------------------------------");
  }

  protected void read(String file) {
    createReader(file);
    long readStart = System.currentTimeMillis();
    logger.info("Start reading packet queue");

    // get filename only to map parquet row to pcap file
    String fileName = extractPcapFile(file);

    long counter = 0;
    for (Packet currentPacket : pcapReader) {
      counter++;
      if (counter % 100000 == 0) {
        logger.info("Read " + counter + " packets. queue: {} combinations", sharedQueue.size());
      }
      if (currentPacket != null && currentPacket.getIpVersion() != 0) {

        if ((currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V4)
            || (currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V6)) {
          // handle icmp
          PacketCombination pc = new PacketCombination(currentPacket, null, serverInfo, false, fileName);
          addToQueue(pc);
        } else {
          DNSPacket dnsPacket = (DNSPacket) currentPacket;
          if (dnsPacket.getMessage() == null) {
            // skip malformed packets
            logger.error("Drop packet with no dns message");
            malformed++;
            continue;
          }

          if (dnsPacket.getMessageCount() > 1) {
            multiCounter = multiCounter + dnsPacket.getMessageCount();
          }

          for (Message msg : dnsPacket.getMessages()) {
            // get qname from request which is part of the cache lookup key
            String qname = null;
            if (msg != null && msg.getQuestions() != null && msg.getQuestions().size() > 0) {
              qname = msg.getQuestions().get(0).getqName();
            }
            // put request into map until we find matching response, with a key based on: query id,
            // qname, ip src, tcp/udp port
            // add time for possible timeout eviction
            if (msg != null && msg.getHeader().getQr() == MessageType.QUERY) {
              queryCounter++;

              // check for ixfr/axfr request
              if (msg.getQuestions().size() > 0
                  && (msg.getQuestions().get(0).getqType() == ResourceRecordType.AXFR
                  || msg.getQuestions().get(0).getqType() == ResourceRecordType.IXFR)) {

                logger.debug("Detected zonetransfer for: {}", dnsPacket.getFlow());
                // keep track of ongoing zone transfer, we do not want to store all the response
                // packets for an ixfr/axfr.
                activeZoneTransfers.put(new RequestKey(msg.getHeader().getId(), null,
                    dnsPacket.getSrc(), dnsPacket.getSrcPort()), 0);
              }

              RequestKey key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getSrc(),
                  dnsPacket.getSrcPort(), System.currentTimeMillis());
              _requestCache.put(key, new MessageWrapper(msg, dnsPacket, fileName));
            } else {
              // try to find the request
              responseCounter++;

              // check for ixfr/axfr response, the query might be missing from the response
              // so we cannot use the qname for matching.
              assert msg != null;
              RequestKey key = new RequestKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
                  dnsPacket.getDstPort());
              if (activeZoneTransfers.containsKey(key)) {
                // this response is part of an active zonetransfer.
                // only let the first response continue, reuse the "time" field of the RequestKey to
                // keep track of this.
                Integer ztResponseCounter = activeZoneTransfers.get(key);
                if (ztResponseCounter > 0) {
                  // do not save this msg, drop it here, continue with next msg.
                  continue;
                } else {
                  // 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
                  // has been processed
                  activeZoneTransfers.put(key, 1);
                }
              }

              key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getDst(),
                  dnsPacket.getDstPort());
              MessageWrapper request = _requestCache.remove(key);
              // check to see if the request msg exists, at the start of the pcap there may be
              // missing queries

              if (request != null && request.getPacket() != null && request.getMessage() != null) {
                PacketCombination combination = new PacketCombination(request.getPacket(), request.getMessage(),
                    serverInfo, dnsPacket, msg, false, fileName);
                addToQueue(combination);
              } else {
                // no request found, this could happen if the query was in previous pcap
                // and was not correctly decoded, or the request timed out before server
                // could send a response.
                logger.debug("Found no request for response");
                noQueryFoundCounter++;
                PacketCombination combination = new PacketCombination(null, null,
                    serverInfo, dnsPacket, msg, false, fileName);
                addToQueue(combination);
              }
            }
          }
        } // end of dns packet
      }
    }
    logger.info("Processing time: " + (System.currentTimeMillis() - readStart) + "ms");
    logger.debug("Done with decoding, start cleanup");

    // clear expired cache entries
    pcapReader.clearCache(config.getTcpFlowCacheTimeoutInMs(), config.getFragmentedIpCacheTimeoutInMs());
    pcapReader.close();
  }

  protected void purgeCache() {
    // remove expired entries from _requestCache
    Iterator<RequestKey> iter = _requestCache.keySet().iterator();
    long now = System.currentTimeMillis();

    while (iter.hasNext()) {
      RequestKey key = iter.next();
      // add the expiration time to the key and see if this leads to a time which is after the current time.
      if ((key.getTime() + config.getCacheTimeoutInMs()) <= now) {
        // remove expired request;
        MessageWrapper mw = _requestCache.get(key);
        iter.remove();
        if (mw.getMessage() != null && mw.getMessage().getHeader().getQr() == MessageType.QUERY) {
          PacketCombination combo = new PacketCombination(mw.getPacket(), mw.getMessage(),
              serverInfo, true, mw.getFilename());
          addToQueue(combo);
          purgeCounter++;
        } else {
          logger.debug("Cached response entry timed out, request might have been missed");
          noQueryFoundCounter++;
        }
      }
    }
    logger.info("Marked {} expired queries from request cache to output file with rcode no response", purgeCounter);
  }

  protected void addToQueue(PacketCombination combination) {
    try {
      if (sharedQueue.offer(combination, 15, TimeUnit.SECONDS)) {
        added++;
        if (added % LOG_FREQUENCY == 0) {
          logger.debug("combinations added to the queue = {} queue.size = {}", added, sharedQueue.size());
        }
      } else {
        timeOuts++;
        if (timeOuts % 100 == 1) {
          logger.error("timeout adding combination to queue. We had {} time outs so far.", timeOuts);
        }
      }
    } catch (InterruptedException e) {
      logger.error("interrupted while adding combination to queue");
      purgeCounter++;
    }
  }


  private String extractPcapFile(String file) {
    try {
      return FileUtils.getFile(file).getName();
    } catch (Exception e) {
      logger.error("Could not get filename of " + file, e);
      return "error";
    }
  }

  // copied from nl.sidn.pcap.load.LoaderThread
  protected void createReader(String file) {
    logger.info("Start loading queue from file:" + file);
    try {
      File f = FileUtils.getFile(file);
      logger.info("Load data for server: " + serverInfo);

      FileInputStream fis = FileUtils.openInputStream(f);
      int bufSize = config.getBufferSizeInBytes();
      InputStream decompressor = getDecompressorStreamWrapper(fis, file, bufSize);

      DataInputStream dis = new DataInputStream(decompressor);
      pcapReader.init(dis);
    } catch (IOException e) {
      logger.error("Error opening pcap file: " + file, e);
      throw new RuntimeException("Error opening pcap file: " + file);
    }
  }

  /**
   * wraps the inputstream with a decompressor based on a filename ending
   *
   * @param in       The input stream to wrap with a decompressor
   * @param filename The filename from which we guess the correct decompressor
   * @return the compressor stream wrapped around the inputstream. If no decompressor is found,
   * returns the inputstream as-is
   */
  public InputStream getDecompressorStreamWrapper(InputStream in, String filename, int bufSize)
      throws IOException {
    String filenameLower = filename.toLowerCase();
    if (filenameLower.endsWith(".xz")) {
      return new XZCompressorInputStream(in);
    }
    if (filenameLower.endsWith(".pcap")) {
      return in;
    }
    if (filenameLower.endsWith(".gz")) {
      return new GZIPInputStream(in, bufSize);
    }

    throw new IOException("Could not open file with unknown extension: " + filenameLower);
  }

  public String createStateFileName() {
    return config.getStateDir() + "/" + DECODER_STATE_FILE + "-" + serverInfo.getFullname() + ".bin";
  }

  /**
   * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
   */
  private void persistState() {
    Kryo kryo = new Kryo();
    Output output;
    String file = createStateFileName();
    logger.info("persisting decoder state to {}", file);
    try {
      // persist tcp state
      output = new Output(new FileOutputStream(file));
      Map<TCPFlow, Collection<SequencePayload>> flows = pcapReader.getFlows().asMap();
      // convert to std java map and collection
      Map<TCPFlow, Collection<SequencePayload>> pmap = new HashMap<>();
      Iterator<TCPFlow> iter = flows.keySet().iterator();
      while (iter.hasNext()) {
        TCPFlow tcpFlow = iter.next();
        Collection<SequencePayload> payloads2Persist = flows.get(tcpFlow);
        Collection<SequencePayload> payloads = new ArrayList<>(payloads2Persist);
        pmap.put(tcpFlow, payloads);
      }
      kryo.writeObject(output, pmap);

      // persist IP datagrams
      Map<Datagram, Collection<DatagramPayload>> datagrams = pcapReader.getDatagrams().asMap();
      // convert to std java map and collection
      Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<>();
      Iterator<Datagram> ipIter = datagrams.keySet().iterator();
      while (iter.hasNext()) {
        Datagram dg = ipIter.next();
        Collection<DatagramPayload> datagramPayloads = datagrams.get(dg);
        Collection<DatagramPayload> datagrams2persist = new ArrayList<>(datagramPayloads);
        outMap.put(dg, datagrams2persist);
      }

      kryo.writeObject(output, outMap);

      // persist request cache
      kryo.writeObject(output, _requestCache);

      // persist running statistics
      // MetricManager.getInstance().getMetricPersistenceManager().persist(kryo, output);

      output.close();
      logger.info("------------- State persistence stats --------------");
      logger.info("Data is persisted to " + file);
      logger.info("Persist " + pmap.size() + " TCP flows");
      logger.info("Persist " + pcapReader.getDatagrams().size() + " Datagrams");
      logger.info("Persist request cache " + _requestCache.size() + " DNS requests");
      logger.info("----------------------------------------------------");
    } catch (Exception e) {
      logger.error("Error saving decoder state to file: " + file, e);
    }
  }

  @SuppressWarnings("unchecked")
  public void loadState() {
    Kryo kryo = new Kryo();
    String file = createStateFileName();
    logger.info("loading decoder state from {}", file);
    if (!Files.exists(Paths.get(file))) {
      logger.info("No state found at " + file);
      return;
    }
    try {
      Input input = new Input(new FileInputStream(file));

      // read persisted TCP sessions
      Multimap<TCPFlow, SequencePayload> flows = TreeMultimap.create();
      HashMap<TCPFlow, Collection<SequencePayload>> map = kryo.readObject(input, HashMap.class);
      for (TCPFlow flow : map.keySet()) {
        Collection<SequencePayload> payloads = map.get(flow);
        for (SequencePayload sequencePayload : payloads) {
          flows.put(flow, sequencePayload);
        }
      }
      logger.info("flows loaded: {}", flows.size());
      pcapReader.setFlows(flows);

      // read persisted IP datagrams
      Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> datagrams =
          TreeMultimap.create();
      HashMap<Datagram, Collection<DatagramPayload>> inMap = kryo.readObject(input, HashMap.class);
      for (Datagram flow : inMap.keySet()) {
        Collection<DatagramPayload> payloads = inMap.get(flow);
        for (DatagramPayload dgPayload : payloads) {
          datagrams.put(flow, dgPayload);
        }
      }
      logger.info("datagrams loaded: {}", datagrams.size());
      pcapReader.setDatagrams(datagrams);

      // read in previous request cache
      _requestCache = kryo.readObject(input, HashMap.class);
      logger.info("requestCache loaded: {}", _requestCache.size());

      long oldest = Long.MAX_VALUE;
      long newest = Long.MIN_VALUE;

      for (RequestKey requestKey : _requestCache.keySet()) {
        oldest = Math.min(requestKey.getTime(), oldest);
        newest = Math.max(requestKey.getTime(), newest);
      }
      logger.info("oldest in request cache: {} = {}", oldest, new DateTime(oldest));
      logger.info("newest in request cache: {} = {}", newest, new DateTime(newest));

      // read running statistics
      // MetricManager.getInstance().getMetricPersistenceManager().load(kryo, input);

      input.close();
      logger.info("------------- Loader state stats ------------------");
      logger.info("Loaded TCP state " + pcapReader.getFlows().size() + " TCP flows");
      logger.info("Loaded Datagram state " + pcapReader.getDatagrams().size() + " Datagrams");
      logger.info("Loaded Request cache " + _requestCache.size() + " DNS requests");
      logger.info("----------------------------------------------------");
    } catch (Exception e) {
      logger.error("Error opening state file, continue without loading state: " + file, e);
    }
  }

  public void printState(int limit) {
    long oldest = Long.MAX_VALUE;
    long newest = Long.MIN_VALUE;

    long oldestRequest = Long.MAX_VALUE;
    long newestRequest = Long.MIN_VALUE;

    int printed = 0;
    logger.info("======== requestCache = {}", _requestCache.size());

    for (RequestKey requestKey : _requestCache.keySet()) {
      oldest = Math.min(requestKey.getTime(), oldest);
      newest = Math.max(requestKey.getTime(), newest);

      MessageWrapper msg = _requestCache.get(requestKey);
      long ts = msg.getPacket().getTs() * 1000;
      oldestRequest = Math.min(ts, oldestRequest);
      newestRequest = Math.max(ts, newestRequest);

      if (printed < limit) {
        logger.info("requestKey: {} : {} : {}", new DateTime(ts), new DateTime(requestKey.getTime()), requestKey);
        printed++;
      }
    }
    logger.info("oldest in request cache: {} = {}", oldest, new DateTime(oldest));
    logger.info("newest in request cache: {} = {}", newest, new DateTime(newest));

    logger.info("oldest request: {} = {}", oldestRequest, new DateTime(oldestRequest));
    logger.info("newest request: {} = {}", newestRequest, new DateTime(newestRequest));

    logger.info("==== TCP flows: {} =============", pcapReader.getFlows().size());
    for (Map.Entry<TCPFlow, SequencePayload> entry : pcapReader.getFlows().entries()) {
      logger.info("TCP flow: {} => {}", entry.getKey(), entry.getValue());
    }

    logger.info("==== Datagrams: {} =============", pcapReader.getDatagrams().size());
    for (Map.Entry<Datagram, DatagramPayload> entry : pcapReader.getDatagrams().entries()) {
      logger.info("Datagram: {} => {}", entry.getKey(), entry.getValue());
    }
  }

}
