package be.dnsbelgium.data.pcap.service;

import au.com.bytecode.opencsv.CSVWriter;
import nl.sidn.dnslib.types.ResourceRecordType;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class QueryTypesTest {

  @Test
  public void generateCSV() throws IOException {

    /*
    CREATE EXTERNAL TABLE parquet.recordtypes(
      qtype int,
      name string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ("separatorChar" = ",", "escapeChar" = "\\")
    LOCATION
      's3://parquet.output.tests/helper-tables/'
    TBLPROPERTIES ('has_encrypted_data'='false')

     */

    Path path = Files.createTempFile("record-types", ".csv");
    CSVWriter writer = new CSVWriter(new FileWriter(path.toFile()), ',');

    for (ResourceRecordType recordType : ResourceRecordType.values()) {
      String value = "" + recordType.getValue();
      String name = recordType.name();
      String[] columns = {value, name};
      writer.writeNext(columns);
    }
    writer.flush();
    writer.close();

    System.out.println("CSV written in " + path);

  }
}
