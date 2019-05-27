package be.dnsbelgium.data.pcap.reader;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class PcapReaderConfig {

  // max lifetime for cached packets (in milliseconds)
  private final int cacheTimeoutInMs;

  // max lifetime for TCP flows (in milliseconds)
  private final int tcpFlowCacheTimeoutInMs;

  // max lifetime for fragmented IP packets
  private final int fragmentedIpCacheTimeoutInMs;

  private final String stateDir;
  private final int queueSize;
  private int bufferSizeInBytes;

  public final static int DEFAULT_TIME_OUT = 5000;
  public final static int DEFAULT_QUEUE_SIZE = 100_000;
  public final static int DEFAULT_PCAP_READER_BUFFER_SIZE = 65536;
  public final static String DEFAULT_DECODER_DIR = "/data/pcap-to-athena/pcap-decoder-state";

  private static final Logger logger = getLogger(PcapReaderConfig.class);

  @Autowired
  public PcapReaderConfig(
      @Value("${pcap.reader.cache.timeout.ms: #{pcapReaderConfig.DEFAULT_TIME_OUT}}") int cacheTimeoutInMs,
      @Value("${pcap.reader.tcp.flow.cache.timeout.ms: #{pcapReaderConfig.DEFAULT_TIME_OUT}}") int tcpFlowCacheTimeoutInMs,
      @Value("${pcap.reader.fragemented.ip.cache.timeout.ms: #{pcapReaderConfig.DEFAULT_TIME_OUT}}") int fragmentedIpCacheTimeoutInMs,
      @Value("${pcap.reader.bufferSize.bytes: #{pcapReaderConfig.DEFAULT_PCAP_READER_BUFFER_SIZE}}") int bufferSizeInBytes,
      @Value("${pcap.decoder.state.dir: #{pcapReaderConfig.DEFAULT_DECODER_DIR}}") String decoderStateDir,
      @Value("${pcap.reader.queue.size: #{pcapReaderConfig.DEFAULT_QUEUE_SIZE}}") int queueSize
  ) throws IOException {
    this.cacheTimeoutInMs = cacheTimeoutInMs;
    this.tcpFlowCacheTimeoutInMs = tcpFlowCacheTimeoutInMs;
    this.fragmentedIpCacheTimeoutInMs = fragmentedIpCacheTimeoutInMs;
    this.stateDir = decoderStateDir;
    this.bufferSizeInBytes = bufferSizeInBytes;
    this.queueSize = queueSize;

    // Check config
    Path path = Paths.get(this.stateDir);
    if (!Files.exists(path)) {
      try {
        Files.createDirectory(path);
      } catch (IOException e) {
        logger.error("{} does not exists or it is not accessible", path);
        throw e;
      }
    }
  }

  public int getCacheTimeoutInMs() {
    return cacheTimeoutInMs;
  }

  public int getTcpFlowCacheTimeoutInMs() {
    return tcpFlowCacheTimeoutInMs;
  }

  public int getFragmentedIpCacheTimeoutInMs() {
    return fragmentedIpCacheTimeoutInMs;
  }

  public String getStateDir() {
    return stateDir;
  }

  public int getBufferSizeInBytes() {
    // sanity check
    if (bufferSizeInBytes <= 512) {
      // use default
      return DEFAULT_PCAP_READER_BUFFER_SIZE;
    }
    return bufferSizeInBytes;
  }

  public int getQueueSize() {
    return queueSize;
  }

  @PostConstruct
  public void logConfig() {
    logger.info("====== PcapReaderConfig ==============");
    logger.info(" cacheTimeoutInMs             = {}", cacheTimeoutInMs);
    logger.info(" tcpFlowCacheTimeoutInMs      = {}", tcpFlowCacheTimeoutInMs);
    logger.info(" fragmentedIpCacheTimeoutInMs = {}", fragmentedIpCacheTimeoutInMs);
    logger.info(" bufferSizeInBytes            = {}", bufferSizeInBytes);
    logger.info(" stateDir                     = {}", stateDir);
    logger.info(" queueSize                    = {}", queueSize);
  }
}
