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
