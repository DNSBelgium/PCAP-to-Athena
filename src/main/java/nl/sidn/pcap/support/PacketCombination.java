package nl.sidn.pcap.support;

import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;

public class PacketCombination {

  public static final PacketCombination NULL = new PacketCombination(null,null, null, false, null);

  public static final PacketCombination FAILURE = new PacketCombination(null,null, null, true, null);

  private ServerInfo server;
  private String pcapFilename;
  private Packet request;
  private Message requestMessage;
  private Packet response;
  private Message responseMessage;
  //true if this packet has expired from cache
  private boolean expired;

  public PacketCombination(Packet request, Message requestMessage, ServerInfo server, boolean expired, String pcapFilename) {
    this(request, requestMessage, server, null, null, expired, pcapFilename);
  }

  public PacketCombination(Packet request, Message requestMessage, ServerInfo server, Packet response, Message responseMessage,
                           boolean expired, String pcapFilename) {
    this.request = request;
    this.response = response;
    this.requestMessage = requestMessage;
    this.responseMessage = responseMessage;
    this.server = server;
    this.expired = expired;
    this.pcapFilename = pcapFilename;
  }

  public Packet getRequest() {
    return request;
  }

  public Packet getResponse() {
    return response;
  }

  public Message getRequestMessage() {
    return requestMessage;
  }

  public Message getResponseMessage() {
    return responseMessage;
  }

  public ServerInfo getServer() {
    return server;
  }

  public boolean isExpired(){
    return expired;
  }

  public String getPcapFilename() {
    return pcapFilename;
  }

}
