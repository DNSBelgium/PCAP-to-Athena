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

package be.dnsbelgium.data.pcap.ip;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SubnetChecks {

  private Map<String, SubnetCheck> subnetChecks = new HashMap<>();

  public void add(String column, SubnetCheck check) {
    subnetChecks.put(column, check);
  }

  public SubnetCheck get(String column) {
    return subnetChecks.get(column);
  }

  public Set<String> getColumns() {
    return Collections.unmodifiableSet(subnetChecks.keySet());
  }

  public void updateAll() {
    for (SubnetCheck check : subnetChecks.values()) {
      check.update();
    }
  }


}
