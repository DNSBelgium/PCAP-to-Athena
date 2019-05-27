package be.dnsbelgium.data.pcap.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(locations="classpath:test-application.properties")
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
public class S3Tools {

  @SuppressWarnings({"unused", "SpringJavaInjectionPointsAutowiringInspection"})
  @Autowired
  private AmazonS3 amazonS3;

  private static final Logger logger = getLogger(S3Tools.class);
  private final static String REGEX = "(?<server>[^/]+)/(?<ddmmyyyy>\\d{2}-\\d{2}-\\d{4})/(?<epoch>\\d+)_(?<filenamePart>.*)_DONE.gz";
  private final static Pattern PATTERN = Pattern.compile(REGEX);

  @Test
  @Ignore
  public void listFiles() {

    String bucketName = "";
    String prefix = "";
    logger.info("Listing all files in {}/{}", bucketName, prefix);

    int listRequests = 0;
    int objects = 0;
    int folderCount = 0;
    int files = 0;
    int done_gz = 0;
    int notMatchingRegex = 0;
    Map<String, AtomicInteger> perServer = new HashMap<>();
    Map<String, AtomicInteger> perStorageClass = new HashMap<>();
    Map<String, AtomicInteger> perDay = new HashMap<>();
    Map<String, AtomicInteger> perMonth = new HashMap<>();
    Map<String, AtomicInteger> perServerMonth = new HashMap<>();
    Map<String, AtomicInteger> perMonthStorageClass = new HashMap<>();
    int compressedCount = 0;
    int glacier = 0;
    ObjectListing objectListing = amazonS3.listObjects(bucketName, prefix);
    listRequests++;
    ZonedDateTime started = ZonedDateTime.now();

    while (true) {

      List<String> folders = objectListing.getCommonPrefixes();
      logger.info("folders = {}", folders);

      for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
        objects++;

        if (summary.getKey().endsWith("/")) {
          folderCount++;
        } else {
          files++;

          if (summary.getKey().endsWith("DONE.gz")) {
            done_gz++;
          }

          String key = summary.getKey();
          //String fileName = new File(key).toPath().getFileName().toString();
          Matcher matcher = PATTERN.matcher(key);
          boolean compressed = key.endsWith(".gz");
          if (compressed) {
            compressedCount++;
          }
          String storageClass = summary.getStorageClass();
          increment(perStorageClass, storageClass);
          if (matcher.find()) {
            String server = matcher.group("server");
            String dd_mm_yyyy = matcher.group("ddmmyyyy");
            LocalDate date = LocalDate.parse(dd_mm_yyyy, DateTimeFormatter.ofPattern("ddMMyyyy"));
            String month = date.format(DateTimeFormatter.ofPattern("yyyy'_'MM"));
            increment(perServer, server);
            increment(perDay, dd_mm_yyyy);
            increment(perMonth, month);
            increment(perServerMonth, server + "_" + month);
            increment(perMonthStorageClass, month + "_" + storageClass);
          } else {
            logger.warn("Not matching regex: {}", key);
            notMatchingRegex++;
          }
        }
      }
      if (started.isBefore(ZonedDateTime.now().minusMinutes(60))) {
        logger.warn("Already running for 60 minutes => aborting");
        break;
      }
      if (objectListing.isTruncated()) {
        objectListing = amazonS3.listNextBatchOfObjects(objectListing);
        listRequests++;
      } else {
        break;
      }
    }
    logger.info("bucketName = {}", bucketName);
    logger.info("prefix = {}", prefix);
    logger.info("listRequests = {}", listRequests);
    logger.info("perServerMonth = {}", perServerMonth);
    logger.info("perMonth = {}", perMonth);
    logger.info("perDay = {}", perDay);
    logger.info("perServer = {}", perServer);
    logger.info("perStorageClass = {}", perStorageClass);
    logger.info("glacier = {}", glacier);
    logger.info("compressedCount = {}", compressedCount);
    logger.info("files = {}", files);
    logger.info("folders = {}", folderCount);
    logger.info("objects = {}", objects);
    logger.info("listRequests = {}", listRequests);

    print(perServer, "per server");
    print(perMonth, "per month");
    print(perStorageClass, "per storage class");
    print(perServerMonth, "per server per month");
    print(perMonthStorageClass, "per month / storage class");

    logger.info("done_gz = {}", done_gz);
    logger.info("notMatchingRegex = {}", notMatchingRegex);

  }

  private static void print(Map<String, AtomicInteger> counters, String desc) {
    List<String> keys = new ArrayList<>(counters.keySet());
    Collections.sort(keys);
    int total = 0;
    logger.info("******* {} : {} keys *****", desc, keys.size());
    for (String key : keys) {
      logger.info("  {} =>  {}", key, counters.get(key));
      total += counters.get(key).get();
    }
    logger.info("  total = {}", total);
  }

  private static void increment(Map<String, AtomicInteger> counters, String key) {
    AtomicInteger counter = counters.get(key);
    if (counter == null) {
      counter = new AtomicInteger();
      counters.put(key, counter);
    }
    counter.incrementAndGet();
  }

}
