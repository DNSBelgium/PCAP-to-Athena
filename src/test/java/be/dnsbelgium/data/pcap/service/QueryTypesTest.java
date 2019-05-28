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
