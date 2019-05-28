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
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;


@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
@TestPropertySource(locations="classpath:test-application.properties")
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
public class AthenaTest {

  private static final Logger logger = getLogger(AthenaTest.class);

  @Autowired
  private AthenaTools tools;

  @Value("${athena.test.create.table.s3.path}")
  private String s3path;

  // The Athena database to be used in these tests.
  // Avoid database names containning hyphens cause some statements will not work, even when quoted.
  private static final String DATABASE_NAME = "integrationtests";
  private static final String TABLE_NAME = "TestTable1";

 @After
  public void after() {
    tools.dropTable(DATABASE_NAME, TABLE_NAME);
  }

  @Test
  public void readTables() {
    getTableNames();
  }

  private List<String> getTableNames() {
    List<String> tableNames = tools.getTables(DATABASE_NAME);
    for (String table : tableNames) {
      logger.info("  table: {}", table);
    }
    return tableNames;
  }

  @Test
  public void testCreateAndDropTable() {
    logger.info("Tables BEFORE creating {}", TABLE_NAME);
    int before = getTableNames().size();
    tools.createDnsQueryTable(DATABASE_NAME, TABLE_NAME, s3path);
    logger.info("Tables AFTER creating {}", TABLE_NAME);
    List<String> afterCreate = getTableNames();
    assertEquals("We should have an extra table", before + 1, afterCreate.size());
    assertTrue("new table should be in the list", afterCreate.contains(TABLE_NAME.toLowerCase()));
    tools.dropTable(DATABASE_NAME, TABLE_NAME);
    List<String> afterDrop = getTableNames();
    assertEquals("We should have same number of table as before create", before, afterDrop.size());
    assertFalse("new table should be gone", afterDrop.contains(TABLE_NAME.toLowerCase()));
  }


  @Test
  public void addPartitions() {
    tools.createDnsQueryTable(DATABASE_NAME, TABLE_NAME, s3path);
    long rowCount = countRows();
    assertEquals("we should find 0 rows before adding partitions", 0, rowCount);

    List<String> partitions = tools.getPartitions(DATABASE_NAME, TABLE_NAME);
    logger.info("BEFORE adding 1st: partitions = {}", partitions);
    assertEquals("we should find no partitions", 0, partitions.size());

    LocalDate day24 = LocalDate.of(2018, 10, 24);
    ServerInfo server = new ServerInfo("milano1.dns.be", "milano1", "milano");
    tools.addPartition(day24, server, DATABASE_NAME, TABLE_NAME, s3path);
    partitions = tools.getPartitions(DATABASE_NAME, TABLE_NAME);
    logger.info("AFTER adding first: partitions = {}", partitions);
    assertEquals("we should find 1 partition", 1, partitions.size());
    assertEquals("year=2018/month=10/day=24/server=milano1", partitions.get(0));
    long afterAddingFirstPartition = countRows();
    assertEquals("rows in first partition", 30, afterAddingFirstPartition);

    LocalDate day25 = LocalDate.of(2018, 10, 25);
    tools.addPartition(day25, server, DATABASE_NAME, TABLE_NAME, s3path);
    long afterAddingSecondPartition = countRows();
    assertEquals("rows after adding 2nd partition", 13724, afterAddingSecondPartition);
    partitions = tools.getPartitions(DATABASE_NAME, TABLE_NAME);
    logger.info("AFTER adding 2nd: partitions = {}", partitions);
    assertEquals("we should find 2 partitions", 2, partitions.size());
    assertTrue(partitions.contains("year=2018/month=10/day=24/server=milano1"));
    assertTrue(partitions.contains("year=2018/month=10/day=25/server=milano1"));

    // read some rows
    String sql = String.format("select * from %s.%s limit 10", DATABASE_NAME, TABLE_NAME);
    List<Map<String, Object>> results = tools.query(sql);
    logger.info("results = {}", results.size());
    for (Map<String, Object> row : results) {
      logger.info("row = {}", row);
    }
    assertEquals("we should have retrieved 10 rows", 10, results.size());
    assertTrue("result should have an ID column", results.get(0).containsKey("id"));
    assertTrue("result should have a qname column", results.get(0).containsKey("qname"));
  }

  private long countRows() {
    long rowCount = tools.countRows(DATABASE_NAME, TABLE_NAME);
    logger.info("Found {} rows in {}", rowCount, TABLE_NAME);
    return rowCount;
  }

  @Test
  public void loadPartitions() {
    tools.createDnsQueryTable(DATABASE_NAME, TABLE_NAME, s3path);
    long rowCount = countRows();
    logger.info("Before loading partitions: rowCount = ", rowCount);
    assertEquals("we should find 0 rows before loading partitions", 0, rowCount);
    tools.detectNewPartitions(DATABASE_NAME, TABLE_NAME);
    rowCount = countRows();
    logger.info("After loading partitions: rowCount = ", rowCount);
    List<String> partitions = tools.getPartitions(DATABASE_NAME, TABLE_NAME);
    logger.info("partitions = {}", partitions);
  }

}
