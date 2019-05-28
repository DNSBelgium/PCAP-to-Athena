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

package be.dnsbelgium.data.pcap.aws.athena;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.time.LocalDate;

import static org.slf4j.LoggerFactory.getLogger;

public class AthenaStatements {

  private static final Logger logger = getLogger(AthenaStatements.class);

  /**
   * Generate DDL to add a new partition to an Athena table
   *
   * @param date day for which we want to add a partition
   * @param server       server for which we want to add a partition
   * @param s3Location   name of S3 bucket (should start with 's3://' and end with "/dnsdata/")
   * @param databaseName the name of the Athena database where table resides.
   * @param tableName    the name of the Athena table. Will be lower-cased.
   * @return the DDL to add a new partition to an Athena table
   */
  public String addPartition(LocalDate date, ServerInfo server, String s3Location, String databaseName, String tableName) {

    if (!s3Location.startsWith("s3://")) {
      throw new RuntimeException("s3Location should start with 's3://' but was " + s3Location);
    }
    // TODO maartenb to confirm if it is needed ?
    if (!s3Location.endsWith("/dnsdata/")) {
      throw new RuntimeException("s3Location should end with '/dnsdata/' but was " + s3Location);
    }

    String ddl = String.format(
        "alter table %s.%s add if not exists partition (year='%04d',month='%02d',day='%02d',server='%s') " +
            " location '%s" + "year=%04d/month=%02d/day=%02d/server=%s'",
        databaseName,
        tableName.toLowerCase(),
        date.getYear(), date.getMonthValue(), date.getDayOfMonth(), server.getName().toLowerCase(),
        s3Location,
        date.getYear(), date.getMonthValue(), date.getDayOfMonth(), server.getName().toLowerCase()
    );
    logger.info("addPartition: DDL = {}", ddl);
    return ddl;
  }

  /**
   * @param date day of partition
   * @param server       server of partition
   * @param tableName    name of the Athena table (can contain database name). Will be lower-cased.
   * @return the SQL to count rows in given partition
   */
  public String countRowsInPartition(LocalDate date, ServerInfo server, String tableName) {
    String sql = String.format(
        "select count(*) from %s where year='%04d' and month='%02d' and day = '%02d' and server = '%s'",
        tableName.toLowerCase(),
        date.getYear(), date.getMonthValue(), date.getDayOfMonth(), server.getName().toLowerCase());
    logger.info("countRowsInPartition: sql = {}", sql);
    return sql;
  }

  public String createTable(String databaseName, String tableName, String s3Location) {
    ClassPathResource resource = new ClassPathResource("/sql/athena-create-table.sql", this.getClass());
    String create_table_stmt;

    try {
      create_table_stmt = FileUtils.readFileToString(resource.getFile(), "UTF-8");
    } catch (IOException e) {
      logger.error("Could not load file", e);
      throw new RuntimeException("Could not load file", e);
    }

    create_table_stmt = create_table_stmt.replace("{DATABASENAME}", databaseName);
    create_table_stmt = create_table_stmt.replace("{TABLENAME}", tableName);
    create_table_stmt = create_table_stmt.replace("{S3LOCATION}", s3Location);
    return create_table_stmt;
  }

}
