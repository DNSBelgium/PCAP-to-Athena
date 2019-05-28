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
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

public class AthenaStatementsTest {

  private LocalDate dmy;
  private ServerInfo server;
  private AthenaStatements statements;

  @Before
  public void before() {
    dmy = LocalDate.of(2018, 9, 02);
    server = new ServerInfo("milano1.dns.be", "Milano1", "milano");
    statements = new AthenaStatements();
  }

  @Test
  public void addPartition() {
    String ddl = statements.addPartition(dmy, server, "s3://my-bucket/my/path/dnsdata/", "my-db", "myTable");
    assertEquals(
        "alter table my-db.mytable add if not exists partition (year='2018',month='09',day='02',server='milano1') " +
            " location 's3://my-bucket/my/path/dnsdata/year=2018/month=09/day=02/server=milano1'", ddl);
  }

  @Test
  public void countRowsInPartition() {
    String sql = statements.countRowsInPartition(dmy, server, "myTable");
    assertEquals("select count(*) from mytable where year='2018' and month='09' and day = '02' and server = 'milano1'", sql);
  }
}