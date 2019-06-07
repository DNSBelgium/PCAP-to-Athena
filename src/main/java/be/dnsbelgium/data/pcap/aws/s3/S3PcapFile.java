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

package be.dnsbelgium.data.pcap.aws.s3;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

public class S3PcapFile implements Comparable<S3PcapFile> {

  private S3ObjectSummary objectSummary;

  private String server;
  private String fileName;
  private boolean compressed;
  private Instant instant;
  private LocalDate date;
  private String iface;
  private String sequenceNr;
  private File localFile = null;
  private boolean skipped = false;

  // Currently PCAP files are stored in a folder per day using dd-mm-yyyy format (=> folders are not in chronological order)
  // for example: amsterdam1.dns.be/01-06-2018/1527804007_amsterdam1.dns.be.p2p2.pcap4249_DONE.gz
  private final static String REGEX = "(?<server>[^/]+)/(?<ddmmyyyy>\\d{2}-\\d{2}-\\d{4})/(?<epoch>\\d+)_(?<filenamePart>.*)_DONE";

  private final static Pattern PATTERN = Pattern.compile(REGEX);

  // In the future we want to store PCAP (and parquet) files in the following folder structure
  // s3://some-bucket/some-prefix/server=xxxx/year=xxxx/month=xx/day=xx/123_some_filename
  private final static String REGEX_WITH_PARTITIONS = "" +
      "(?<prefix>[^/]*/)?" +
      "server=(?<server>[^/]+)" +
      "/year=(?<yyyy>\\d{4})" +
      "/month=(?<mm>\\d{2})" +
      "/day=(?<dd>\\d{2})" +
      "/(?<epoch>\\d{4}_\\d{2}_\\d{2}_\\d{6})_(?<server2>.+)_(?<interface>[^\\.]+)\\.(?<extension>.*)";

  private final static Pattern PATTERN_WITH_PARTITIONS = Pattern.compile(REGEX_WITH_PARTITIONS);

  // part between epoch_ and _DONE => in example: amsterdam1.dns.be.p2p2.pcap4249
  private final static String NAME_INTERFACE_REGEX = "(?<server>.*\\.be).(?<interface>.*).pcap(?<sequenceNr>\\d+)";

  private final static Pattern NAME_INTERFACE_PATTERN = Pattern.compile(NAME_INTERFACE_REGEX);

  private static final Logger logger = getLogger(S3PcapFile.class);

  /**
   * Tries to extract information from given <code>summary</code>
   *
   * @param summary the S3Summary to parse
   * @return a S3PcapFile if S3 key could be parsed, null otherwise.
   */
  static public S3PcapFile parse(S3ObjectSummary summary) {
    String key = summary.getKey();
    String fileName = new File(key).toPath().getFileName().toString();
    Matcher matcher = PATTERN.matcher(key);
    boolean compressed = key.endsWith(".gz");

    Matcher matcherWithPartitions = PATTERN_WITH_PARTITIONS.matcher(key);
    String iface = null;
    String seqNr = null;

    if (matcher.find()) {
      String server = matcher.group("server");
      String dd_mm_yyyy = matcher.group("ddmmyyyy");
      LocalDate date = LocalDate.parse(dd_mm_yyyy, DateTimeFormatter.ofPattern("dd'-'MM'-'yyyy"));

      Instant instant = extractInstance(matcher);

      // zaventem.dns.be/15-11-2018/1542319052_zaventem.dns.be.em3.pcap5554_DONE.gz]
      String fileNamePart = matcher.group("filenamePart");
      logger.debug("fileNamePart= [{}]", fileNamePart);
      Matcher filenameMatcher = NAME_INTERFACE_PATTERN.matcher(fileNamePart);
      if (filenameMatcher.find()) {
        iface = filenameMatcher.group("interface");
        seqNr = filenameMatcher.group("sequenceNr");
      }
      return new S3PcapFile(summary, server, fileName, compressed, instant, date, iface, seqNr);

    } else if (matcherWithPartitions.find()) {

      logger.info("PCAP uses new folder structure");

      String prefix = matcherWithPartitions.group("prefix");
      String server = matcherWithPartitions.group("server");
      String yyyy = matcherWithPartitions.group("yyyy");
      String mm = matcherWithPartitions.group("mm");
      String dd = matcherWithPartitions.group("dd");
      LocalDate date = LocalDate.parse(yyyy + mm + dd, DateTimeFormatter.BASIC_ISO_DATE);

      // TODO quentinl Check the need to use Instant (iso LocalDateTime) after removing the old pattern
      Instant instant = extractInstance(matcherWithPartitions.group("epoch"));

      iface = matcherWithPartitions.group("interface");

      return new S3PcapFile(summary, server, fileName, compressed, instant, date, iface, null);

    } else {
      logger.warn("S3 key {} does not match regex {} nor {} => return null", key, REGEX, REGEX_WITH_PARTITIONS);
      return null;
    }
  }

  private static Instant extractInstance(Matcher matcher) {
    String epoch = matcher.group("epoch");
    long epochSeconds = Integer.parseInt(epoch);
    return Instant.ofEpochSecond(epochSeconds);
  }

  private static Instant extractInstance(String epoch) {
    return LocalDateTime
        .parse(epoch, DateTimeFormatter.ofPattern("yyyy'_'MM'_'dd'_'HHmmss"))
        .toInstant(ZoneOffset.UTC);
  }

  private S3PcapFile(S3ObjectSummary objectSummary, String server, String fileName, boolean compressed, Instant instant, LocalDate date, String iface, String sequenceNr) {
    this.objectSummary = objectSummary;
    this.server = server;
    this.fileName = fileName;
    this.compressed = compressed;
    this.instant = instant;
    this.date = date;
    this.iface = iface;
    this.sequenceNr = sequenceNr;
  }

  public String determineLocalFileName() {
    File file = new File(objectSummary.getKey());
    String fileName = file.getName();
    if (fileName.endsWith(".pcap.gz") || fileName.endsWith(".pcap")) {
      logger.info("OK, filename has a supported extension: {}", fileName);
    } else {
      if (fileName.contains(".gz")) {
        logger.debug("Filename contains .gz => assuming compressed and appending .pcap.gz since the parquet converter expects it");
        fileName += ".pcap.gz";
      } else {
        logger.debug("Filename does not contain .gz => assuming not compressed and appending .pcap since the parquet converter expects it");
        fileName += ".pcap";
      }
    }
    return fileName;
  }


  @Nullable
  public LocalDate getDate() {
    return date;
  }

  @Nullable
  public String getServer() {
    return server;
  }

  @Nullable
  public String getFileName() {
    return fileName;
  }

  public boolean isCompressed() {
    return compressed;
  }

  @Nullable
  public Instant getInstant() {
    return instant;
  }

  public S3ObjectSummary getObjectSummary() {
    return objectSummary;
  }

  @Override
  public int compareTo(S3PcapFile o) {
    // first compare server names then interface names and then the instants
    int s = server.compareTo(o.server);
    if (s == 0) {
      if (iface != null) {
        s = iface.compareTo(o.iface);
      }
      if (s == 0) {
        return instant.compareTo(o.instant);
      }
    }
    return s;
  }

  public String getKey() {
    if (objectSummary == null) {
      return "<objectSummary is null>";
    }
    return objectSummary.getKey();
  }

  @Override
  public String toString() {
    return "S3PcapFile{" +
        "server='" + server + '\'' +
        ", iface='" + iface + '\'' +
        ", fileName='" + fileName + '\'' +
        ", date=" + date +
        '}';
  }

  public String getNetworkInterface() {
    return iface;
  }

  public String getSequenceNr() {
    return sequenceNr;
  }

  /*
       Return a key that makes more sense
       in the form of <some-prefix>/server=xxxx/year=xxxx/month=xx/day=yy/filename.pcap.gz
     */
  public String improvedKey(String prefix, ServerInfo server) {
    StringBuilder newKey = new StringBuilder();
    newKey.append(prefix);
    if (!newKey.toString().endsWith("/")) {
      newKey.append("/");
    }
    newKey.append("server=");
    newKey.append(server.getFullname());

    LocalDate date = getDate();
    if (date != null) {
      newKey.append(String.format("/year=%04d/month=%02d/day=%02d/%s",
          date.getYear(), date.getMonthValue(), this.date.getDayOfMonth(), getFileName()));
    } else {
      logger.error("PCAP file has no day-month-year : {}", this);
      newKey.append("/").append(getKey());
    }
    logger.debug("old key=[{}] => new key=[{}]", getKey(), newKey.toString());
    return newKey.toString();
  }

  public boolean isDownloaded() {
    return localFile != null && localFile.exists();
  }

  public boolean isFullyDownloaded() {
    if (localFile == null) {
      logger.warn("isFullyDownloaded = false since localFile == null");
      return false;
    }
    if (!localFile.exists()) {
      logger.warn("isFullyDownloaded = false since localFile does not exist");
      return false;
    }
    if (objectSummary.getSize() != localFile.length()) {
      logger.warn("isFullyDownloaded = false since localFile {} not same size as S3 object {}", objectSummary.getSize(), localFile.length());
      return false;
    }
    return true;
  }

  public void setDownloadFolder(File downloadFolder) {
    this.localFile = new File(downloadFolder, determineLocalFileName());
  }

  public File getLocalFile() {
    return localFile;
  }

  public boolean isSkipped() {
    return skipped;
  }

  public void setSkipped(boolean skipped) {
    this.skipped = skipped;
  }

  public long size() {
    return objectSummary.getSize();
  }
}
