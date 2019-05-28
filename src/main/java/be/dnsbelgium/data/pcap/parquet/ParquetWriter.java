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
