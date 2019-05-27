package be.dnsbelgium.data.pcap.aws.athena;

import be.dnsbelgium.data.pcap.model.ServerInfo;
import com.simba.athena.jdbc42.DataSource;
import com.simba.athena.support.LogLevel;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class AthenaTools {

  private static final String ATHENA_DS = "com.simba.athena.jdbc42.DataSource";
  private static final String AwsCredentialsProviderClass = "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain";

  @Value("${athena.workgroup}")
  private String workgroup;

  @Value("${athena.driver.name}")
  private String driverName;

  @Value("${athena.url}")
  private String url;

  @Value("${athena.log.path}")
  private String logPath;

  @Value("${athena.output.location}")
  private String outputLocation;

  private AthenaStatements statements = new AthenaStatements();

  private JdbcTemplate template;

  private static final Logger logger = getLogger(AthenaTools.class);

  @PostConstruct
  public void init() {
    logConfig();
    logger.info("creating AthenaTools instance => loading Athena JDBC driver ...");
    setLogLevel();
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      logger.error("Failed to load Athena JDBC driver", e);
      throw new RuntimeException("Failed to load Athena JDBC driver", e);
    }
    DataSource ds = new com.simba.athena.jdbc.DataSource();
    ds.setCustomProperty("LogPath", logPath);
    ds.setCustomProperty("LogLevel", "4");
    ds.setCustomProperty("S3OutputLocation", outputLocation);
    ds.setCustomProperty("AwsCredentialsProviderClass", AwsCredentialsProviderClass);
    ds.setCustomProperty("Workgroup", workgroup);
    ds.setURL(url);
    template = new JdbcTemplate(ds);
  }

  private void setLogLevel() {

    LogLevel.getLogLevel("FATAL");

    try {
      Class<?> clazz = Class.forName(ATHENA_DS);
      Constructor<?> ctor = clazz.getConstructor();
      Object object = ctor.newInstance();
      Method method = clazz.getMethod("setCustomProperty", String.class, String.class);
      //method.invoke(object, "LogLevel", "4");
      method.invoke(object, "LogLevel", "OFF");

      logger.info("LogLevel of {} set via reflection", ATHENA_DS);
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      logger.warn("Failed to set loglevel of {}. Exception={} Message={}", ATHENA_DS, e.getClass().getName(), e.getMessage());
    }
  }

  public void logConfig() {
    logger.info("=== Athena settings ==========");
    logger.info(" ${athena.driver.name}     = {}", driverName);
    logger.info(" ${athena.url}             = {}", url);
    logger.info(" ${athena.log.path}        = {}", logPath);
    logger.info(" ${athena.output.location} = {}", outputLocation);
    logger.info(" ${athena.workgroup}       = {}", workgroup);
    logger.info("==============================");
  }

  /**
   * Tell Athena that there is a new partition.
   * You will need access to glue:BatchCreatePartition on resource: arn:aws:glue:eu-west-1:accountnr:catalog
   *
   * @param date day for which we want to add a partition
   * @param server       server for which we want to add a partition
   * @param databaseName the name of the Athena database where table resides.
   * @param tableName    the name of the Athena table. Will be lower-cased.
   * @param s3Location   location on S3 (should start with 's3://' and end with "/dnsdata/")
   */
  public void addPartition(LocalDate date, ServerInfo server, String databaseName, String tableName, String s3Location) {
    logger.info("Adding a partition for day={} and server = {} to table {}.{} in bucket {}",
        date, server, databaseName, tableName, s3Location);
    String ddl = statements.addPartition(date, server, s3Location, databaseName, tableName);
    logger.info("ddl: {}", ddl);
    template.execute(ddl);
  }

  public List<String> getTables(String database) {
    @SuppressWarnings("SqlDialectInspection")
    String sql = "show tables in " + database;
    List<String> tables = template.queryForList(sql, String.class);
    logger.info("Found {} Athena tables in database [{}]", tables.size(), database);
    return tables;
  }

  public List<String> getPartitions(String databaseName, String tableName) {
    String sql = String.format("show partitions %s.%s", databaseName, tableName);
    logger.debug("getPartitions: {}", sql);
    List<String> partitions = template.queryForList(sql, String.class);
    logger.info("Table {}.{} has {} partitions", databaseName, tableName, partitions.size());
    return partitions;
  }

  public void createDnsQueryTable(String databaseName, String tableName, String s3Location) {
    String ddl = statements.createTable(databaseName, tableName, s3Location);
    executeDDL(ddl);
  }

  public void dropTable(String databaseName, String tableName) {
    logger.warn("===== DROPPING Table {}.{} ======", databaseName, tableName);
    String ddl = String.format("drop table %s.%s", databaseName, tableName);
    executeDDL(ddl);
  }

  public void executeDDL(String ddl) {
    logger.info("Executing DDL: {}", ddl);
    try {
      template.execute(ddl);
    } catch (DataAccessException e) {
      logger.error("Failure when executing [" + ddl + "]", e);
      throw e;
    }
  }

  public List<Map<String, Object>> query(String sql, Object... args) {
    logger.info("executing query [{}]", sql);
    List<Map<String, Object>> results = template.queryForList(sql, args);
    if (results.isEmpty()) {
      logger.info("[{}] returned no rows", sql);
    } else {
      int columnCount = results.get(0).size();
      logger.info("[{}] returned {} rows and {} columns", sql, results.size(), columnCount);
    }
    return results;
  }

  public long countRows(String databaseName, String tableName) {
    String sql = String.format("select count(*) from %s.%s", databaseName, tableName);
    Long rowCount = template.queryForObject(sql, Long.class);
    logger.info("countRows in {}.{} => {} rows", databaseName, tableName, rowCount);
    return (rowCount != null ? rowCount : 0);
  }

  /*
    This will force Athena to detect any new partitions (by scanning S3) and can take a long time.
    It's recommended to explicitly add new partitions using #addPartition
   */
  public void detectNewPartitions(String databaseName, String tableName) {
    String ddl = String.format("MSCK REPAIR TABLE %s.%s", databaseName, tableName);
    logger.info("ddl = {}", ddl);
    template.execute(ddl);
    logger.info("partitions loaded");
  }
}
