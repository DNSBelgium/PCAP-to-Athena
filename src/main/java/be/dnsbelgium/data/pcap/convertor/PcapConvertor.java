package be.dnsbelgium.data.pcap.convertor;

import be.dnsbelgium.data.pcap.ip.SubnetChecks;
import be.dnsbelgium.data.pcap.parquet.DNSParquetPacketWriter;
import be.dnsbelgium.data.pcap.aws.s3.ParquetFile;
import be.dnsbelgium.data.pcap.reader.PcapFileReader;
import be.dnsbelgium.data.pcap.reader.PcapReaderConfig;
import be.dnsbelgium.data.pcap.utils.FileHelper;
import be.dnsbelgium.data.pcap.utils.GeoLookupUtil;
import nl.sidn.pcap.support.NamedThreadFactory;
import nl.sidn.pcap.support.PacketCombination;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.*;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Convert local PCAP files to parquet
 */
@Component
public class PcapConvertor {

  private static final Logger logger = getLogger(PcapConvertor.class);

  private final static int LOG_STATUS_COUNT = 100000;

  private final GeoLookupUtil geoLookup;
  private final SubnetChecks subnetChecks;
  private final FileHelper fileHelper;
  private final PcapReaderConfig config;

  @Autowired
  public PcapConvertor(GeoLookupUtil geoLookup, SubnetChecks subnetChecks, PcapReaderConfig config, FileHelper fileHelper) {
    this.geoLookup = geoLookup;
    this.subnetChecks = subnetChecks;
    this.config = config;
    this.fileHelper = fileHelper;
    logger.info("created a PcapConvertor");
  }

  public void logConfig() {
    config.logConfig();
  }

  /**
   * Execute a local conversion job: convert a given set of local PCAP files to a set of Parquet files
   *
   * @param job - the details of the conversion job, should have one or more PCAP files
   * @return - metrics about the job
   * @throws InterruptedException when thread is interrupted
   */
  public Metrics convertToParquet(LocalConversionJob job) throws InterruptedException {
    Metrics metrics = convertToParquetInternal(job);

    File outputFolder = job.getParquetOutputFolder();
    logger.info("Searching all parquet files in {}", outputFolder);
    Collection<File> files = fileHelper.findRecursively(outputFolder, "parquet");

    logger.info("Found {} parquet files", files.size());
    for (File file : files) {
      job.addParquetFile(new ParquetFile(outputFolder, file));
    }
    logger.info("Converted {} PCAP files of {} into {} parquet files",
        job.getPcapFiles().size(), job.getServerInfo().getFullname(), job.getParquetFiles().size());
    return metrics;
  }

  private Metrics convertToParquetInternal(LocalConversionJob job) throws InterruptedException {
    int filesTotal = job.getPcapFiles().size();
    logger.info("starting to convert {} PCAP files from {}", filesTotal, job.getServerInfo());

    // Create shared queue between reading thread and converting thread
    BlockingQueue<PacketCombination> sharedQueue = new ArrayBlockingQueue<>(config.getQueueSize());

    logger.info("Opening DNSParquetPacketWriter with path {}", job.getParquetOutputFolder());
    DNSParquetPacketWriter writer = new DNSParquetPacketWriter(subnetChecks, geoLookup);

    // Read local PCAP files
    PcapFileReader reader = new PcapFileReader(config, job.getServerInfo(), job.getPcapFiles(), sharedQueue);
    ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("PcapFileReader-Thread"));
    executor.submit(reader);

    // Convert readed PCAP files
    try {
      writer.open(job.getParquetOutputFolder().getAbsolutePath());
      pollAndConvertPcapToParquet(job, sharedQueue, writer);
      writer.close();
    } catch (InterruptedException e) {
      logger.error("Interrupted while processing sharedQueue");
    } finally {
      // Shutdown reader thread in case a problem occured in convertor thread
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    Metrics metrics = writer.getMetrics();

    logger.info("Oldest packet found: {}", metrics.getOldestPacket());
    logger.info("Newest packet found: {}", metrics.getNewestPacket());

    for (String column : subnetChecks.getColumns()) {
      int count = metrics.getMatchCount(column);
      logger.info("{} => {} matches", column, count);
    }

    logger.info("Successfully converted {} PCAP files from {}.", job.getPcapFiles().size(), job.getServerInfo());
    return metrics;
  }

  private void pollAndConvertPcapToParquet(LocalConversionJob job, BlockingQueue<PacketCombination> sharedQueue,
                                           DNSParquetPacketWriter dnsParquetPacketWriter) throws InterruptedException {
    PacketCombination combination;
    int combinationCount = 0;

    while ((combination = getNextPacketCombination(sharedQueue)) != PacketCombination.NULL) {

      if (combination == PacketCombination.FAILURE) {
        logger.error("Found FAILURE packet in the queue => PcapFileReader failed to read the PCAP file");
        // TODO quentinl Discuss the usefulness of this.
        break;
      }
      dnsParquetPacketWriter.write(combination);

      if (++combinationCount % LOG_STATUS_COUNT == 0) {
        logger.debug("Written {} combinations. In the queue: {}", combinationCount, sharedQueue.size());
      }
    }
    logger.debug("All PacketCombinations processed from the queue.");
    logger.info("Processed {} combinations from {}", combinationCount, job.getServerInfo());
  }

  private PacketCombination getNextPacketCombination(BlockingQueue<PacketCombination> sharedQueue) throws InterruptedException {
    PacketCombination combination;
    do {
      combination = sharedQueue.poll(30, TimeUnit.SECONDS);
      if (combination == null) {
        logger.debug("Waiting until next combo appears in the queue ...");
      }
    } while (combination == null);
    return combination;
  }


}
