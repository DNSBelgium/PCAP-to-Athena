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

package be.dnsbelgium.data.pcap.model;

public class Query {

  public int id;
  public long unixtime;
  public long time;
  public String qname;
  public String domainName;
  public int len;
  public int frag;
  public int ttl;
  public int ipv;
  public int prot;
  public String src;
  public int srcp;
  public String dst;
  public int dstp;
  public int udp_sum;
  public int dns_len;
  public boolean aa;
  public boolean tc;
  public boolean rd;
  public boolean ra;
  public boolean z;
  public boolean ad;
  public boolean cd;
  public int ancount;
  public int arcount;
  public int nscount;
  public int qdcount;
  public int opcode;
  public int rcode;
  public int qtype;
  public int qclass;
  public String country;
  public String asn;
  public int edns_edp;
  public int edns_version;
  public boolean edns_do;
  public boolean edns_ping;
  public String edns_nsid;
  public String edns_dnssec_dau;
  public String edns_dnssec_dhu;
  public String edns_dnssec_n3u;
  public String edns_client_subnet;
  public String edns_other;
  public String edns_client_subnet_asn;
  public String edns_client_subnet_country;
  public int labels;
  public int res_len;
  public String svr;
  public long time_micro;
  public int resp_frag;
  public int proc_time;
  public boolean is_google;
  public boolean is_opendns;
  public int dns_res_len;
  public String server_location;
  public int edns_padding;
  public String pcap_file;
  public int edns_keytag_count;
  public String edns_keytag_list;
  public boolean q_tc;
  public boolean q_ra;
  public boolean q_ad;
  public int q_rcode;
  public boolean is_cloudflare;
  public boolean is_quad9;

  public String year;
  public String month;
  public String day;
  public String server;


}
