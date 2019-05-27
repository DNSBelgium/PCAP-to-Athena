package be.dnsbelgium.data.pcap.parquet;

import be.dnsbelgium.data.pcap.convertor.Metrics;
import nl.sidn.pcap.support.PacketCombination;

public interface ParquetWriter {

  void open(String path);

  void close();

  /**
   * create 1 parquet record which combines values from the query and the response
   *
   * @param combo the combo to write to parquet
   */
  void write(PacketCombination combo);

  Metrics getMetrics();

}
