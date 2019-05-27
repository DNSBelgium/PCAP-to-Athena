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