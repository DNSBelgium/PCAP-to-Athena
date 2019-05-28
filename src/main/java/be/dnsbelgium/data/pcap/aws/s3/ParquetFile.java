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
import org.slf4j.Logger;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

public class ParquetFile {

  private static final Logger logger = getLogger(ParquetFile.class);

  private final File baseFolder;
  private final File file;
  private final LocalDate date;
  private final String server;

  private final static String REGEX = ".*/year=(?<yyyy>\\d{4})/month=(?<mm>\\d{2})/day=(?<dd>\\d{2})/server=(?<server>[^/]+)/.*";
  private final static Pattern PATTERN = Pattern.compile(REGEX);


  public ParquetFile(File baseFolder, File file) {
    this.baseFolder = baseFolder;
    this.file = file;
    // extract the LocalDate from the path
    String partions = file.getAbsolutePath().replace(file.getName(), "").replace(baseFolder.getAbsolutePath(), "");
    Matcher matcher = PATTERN.matcher(partions);
    if (matcher.find()) {
      date = LocalDate.parse(matcher.group("yyyy") + matcher.group("mm") + matcher.group("dd"), DateTimeFormatter.BASIC_ISO_DATE);
      server = matcher.group("server");
    } else {
      logger.error("Filename {} does not match regex {}", file, REGEX);
      date = null;
      server = null;
    }
  }

  public LocalDate getDate() {
    return date;
  }

  public String getServer() {
    return server;
  }

  public File getBaseFolder() {
    return baseFolder;
  }

  public File getFile() {
    return file;
  }

  public long size() {
    return file.length();
  }

  public String getKey() {
    return file.getAbsolutePath().replace(baseFolder.getAbsolutePath() + "/", "dnsdata/");
  }

  public boolean matches(ServerInfo server) {
    String regexp = "dnsdata/year=\\d{4}/month=\\d{2}/day=\\d{2}/server=" + server.getName() + "/.*\\.parquet";
    String key = getKey();
    if (!key.matches(regexp)) {
      logger.error("Key {} does matches regexp {}", key, regexp);
      return false;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParquetFile that = (ParquetFile) o;
    return baseFolder.equals(that.baseFolder) &&
        file.equals(that.file);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseFolder, file);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ParquetFile.class.getSimpleName() + "[", "]")
        .add("baseFolder=" + baseFolder)
        .add("file=" + file)
        .toString();
  }
}
