package be.dnsbelgium.data.pcap.convertor;

public enum ConversionStatus {

  @SuppressWarnings("unused") NONE,
  BUSY,
  DONE,
  FAILED,
  TODO;

  public boolean is(String str) {
    return this.name().equals(str);
  }

}
