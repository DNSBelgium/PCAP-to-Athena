package be.dnsbelgium.data.pcap.ip;

import java.io.IOException;
import java.util.List;

public interface SubnetFetcher {

  List<String> fetchSubnets() throws IOException;


}
