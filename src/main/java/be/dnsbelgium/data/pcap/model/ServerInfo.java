package be.dnsbelgium.data.pcap.model;

public class ServerInfo extends nl.sidn.pcap.support.ServerInfo {

  public ServerInfo(String fullname) {
    super();
    this.setFullname(fullname);
    this.setName(fullname);
    this.setLocation(fullname);
  }

  public ServerInfo(String fullname, String name, String location) {
    super();
    this.setFullname(fullname);
    this.setName(name);
    this.setLocation(location);
  }

  public String getNormalizedServerName() {
    return getFullname().replaceAll("[^A-Za-z0-9 ]", "_");
  }

  @Override
  public String toString() {
    return "ServerInfo{" +
        "fullname='" + this.getFullname() + '\'' +
        '}';
  }

}
