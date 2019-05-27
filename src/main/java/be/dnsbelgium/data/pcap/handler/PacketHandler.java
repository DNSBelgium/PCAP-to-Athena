package be.dnsbelgium.data.pcap.handler;

import nl.sidn.pcap.packet.Packet;

public interface PacketHandler {

  boolean handle(Packet packet);
}
