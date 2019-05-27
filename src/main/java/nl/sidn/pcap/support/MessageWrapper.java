package nl.sidn.pcap.support;

import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;

public class MessageWrapper {

  private Message message;
  private Packet packet;
  private String filename;

  public MessageWrapper() {
  }

  public MessageWrapper(Message message, Packet packet, String filename) {
    this.message = message;
    this.packet = packet;
    this.filename = filename;
  }

  public Message getMessage() {
    return message;
  }

  public Packet getPacket() {
    return packet;
  }

  public String getFilename() {
    return filename;
  }

}
