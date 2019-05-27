package be.dnsbelgium.data.pcap.parquet;

import be.dnsbelgium.data.pcap.convertor.Metrics;
import be.dnsbelgium.data.pcap.ip.SubnetChecks;
import be.dnsbelgium.data.pcap.utils.AutonomousSystem;
import be.dnsbelgium.data.pcap.utils.GeoLookupUtil;
import com.google.common.base.Joiner;
import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.Question;
import nl.sidn.dnslib.message.records.edns0.*;
import nl.sidn.dnslib.util.DomainParent;
import nl.sidn.dnslib.util.Domaininfo;
import nl.sidn.dnslib.util.NameUtil;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.stats.MetricManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.data.PartitionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// TODO quentinl Make it Autocloseable
public class DNSParquetPacketWriter extends AbstractParquetPacketWriter implements ParquetWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DNSParquetPacketWriter.class);

  private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;
  private static final String SCHEMA = "avro/dns-query.avsc";

  // use an empty list: only strip off the TLD to get "domainname" column
  private List<DomainParent> parents = new ArrayList<>();

  private MetricManager metricManager;

  private final Metrics metrics = new Metrics();

  private SubnetChecks subnetChecks;
  private Set<String> errorMessages = new HashSet<>();

  public DNSParquetPacketWriter(SubnetChecks subnetChecks, GeoLookupUtil geoLookup) {
    super(geoLookup, SCHEMA);
    this.subnetChecks = subnetChecks;
    metricManager = MetricManager.getInstance();
  }

  public Metrics getMetrics() {
    return metrics;
  }

  /**
   * Get the question, from the request packet if not available then from the response, which should
   * be the same.
   *
   * @param reqMessage the request
   * @param respMessage the response
   * @return the Question from request or response packet
   */
  private Question lookupQuestion(Message reqMessage, Message respMessage) {
    if (reqMessage != null && reqMessage.getQuestions().size() > 0) {
      return reqMessage.getQuestions().get(0);
    } else if (respMessage != null && respMessage.getQuestions().size() > 0) {
      return respMessage.getQuestions().get(0);
    }
    // should never get here
    return null;
  }

  private long lookupTime(Packet reqPacket, Packet respPacket) {
    if (reqPacket != null) {
      return reqPacket.getTs();
    } else if (respPacket != null) {
      return respPacket.getTs();
    }
    // should never get here
    return -1;
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   *
   * @param combo the combo to write to parquet
   */
  @Override
  public void write(PacketCombination combo) {

    GenericRecordBuilder builder = newBuilder();

    packetCounter++;
    if (packetCounter % STATUS_COUNT == 0) {
      LOGGER.debug(packetCounter + " packets written to parquet file.");
    }
    Packet reqTransport = combo.getRequest();
    Message requestMessage = combo.getRequestMessage();
    Packet respTransport = combo.getResponse();
    Message respMessage = combo.getResponseMessage();

    // get the question
    Question question = lookupQuestion(requestMessage, respMessage);

    // get the headers from the messages.
    Header requestHeader = null;
    Header responseHeader = null;
    if (requestMessage != null) {
      requestHeader = requestMessage.getHeader();
    }
    if (respMessage != null) {
      responseHeader = respMessage.getHeader();
    }

    // get the time in milliseconds
    long time = lookupTime(reqTransport, respTransport);
    metrics.registerTimestamp(Instant.ofEpochSecond(time));

    // get the qname domain name details
    String normalizedQname = question == null ? "" : filter(question.getqName());
    normalizedQname = StringUtils.lowerCase(normalizedQname);
    Domaininfo domaininfo = NameUtil.getDomain(normalizedQname, parents);
    // check to see it a response was found, if not then save -1 value
    // otherwise use the rcode returned by the server in the response.
    // no response might be caused by rate limiting
    int rcode = RCODE_QUERY_WITHOUT_RESPONSE; // default no reply, use non standard rcode value -1

    // set the nameserver the queries are going to/coming from
    builder.set("svr", combo.getServer().getName());

    // if no anycast location is encoded in the name then the anycast location will be null
    builder.set("server_location", combo.getServer().getLocation());

    // add file name, makes it easier to find the original input pcap
    // in case of of debugging.
    builder.set("pcap_file", combo.getPcapFilename());

    // add meta data
    enrich(reqTransport, respTransport, builder);

    // these are the values that are retrieved from the response
    if (respTransport != null && respMessage != null && responseHeader != null) {
      // use rcode from response
      rcode = responseHeader.getRawRcode();

      builder.set("id", responseHeader.getId()).set("opcode", responseHeader.getRawOpcode())
          .set("aa", responseHeader.isAa()).set("tc", responseHeader.isTc())
          .set("ra", responseHeader.isRa()).set("ad", responseHeader.isAd())
          .set("ancount", (int) responseHeader.getAnCount())
          .set("arcount", (int) responseHeader.getArCount())
          .set("nscount", (int) responseHeader.getNsCount())
          .set("qdcount", (int) responseHeader.getQdCount())
          // size of the complete packet incl all headers
          .set("res_len", respTransport.getTotalLength())
          // size of the dns message
          .set("dns_res_len", respMessage.getBytes());

      // ip fragments in the response
      if (respTransport.isFragmented()) {
        int frags = respTransport.getReassembledFragments();
        builder.set("resp_frag", frags);

        if ((respTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && frags > 1) {
          metrics.incrementResponseUDPFragmentedCount();
        } else if ((respTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && frags > 1) {
          metrics.incrementResponseTCPFragmentedCount();
        }
      }

      // EDNS0 for response
      writeResponseOptions(respMessage, builder);

      // update metric
      metrics.incrementResponseBytes(respTransport.getUdpLength());

      if (!combo.isExpired()) {
        // do not send expired queries, this will cause duplicate timestamps with low values
        // this looks like dips in the grafana graph
        metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_RESPONSE_COUNT, 1, time);
      }
    } // end of response only section

    // values from request OR response now
    // if no request found in the request then use values from the response.
    //noinspection ConstantConditions

    builder.set("rcode", rcode)
        .set("unixtime", time)
        .set("time", time * 1000L)
        .set("time_micro",
            reqTransport != null ? reqTransport.getTsmicros() : respTransport.getTsmicros())
        .set("qname", normalizedQname).set("domainname", domaininfo.name)
        .set("labels", domaininfo.labels)
        .set("src", reqTransport != null ? reqTransport.getSrc() : respTransport.getDst())
        .set("len", reqTransport != null ? reqTransport.getTotalLength() : null)
        .set("ttl", reqTransport != null ? reqTransport.getTtl() : null)
        .set("ipv",
            reqTransport != null ? (int) reqTransport.getIpVersion()
                : (int) respTransport.getIpVersion())
        .set("prot",
            reqTransport != null ? (int) reqTransport.getProtocol()
                : (int) respTransport.getProtocol())
        .set("srcp", reqTransport != null ? reqTransport.getSrcPort() : null)
        .set("dst", reqTransport != null ? reqTransport.getDst() : respTransport.getSrc())
        .set("dstp", reqTransport != null ? reqTransport.getDstPort() : respTransport.getSrcPort())
        .set("udp_sum", reqTransport != null ? reqTransport.getUdpsum() : null)
        .set("dns_len", requestMessage != null ? requestMessage.getBytes() : null);

    // get values from the request only.
    // may overwrite values from the response
    if (reqTransport != null && requestHeader != null) {
      builder.set("id", requestHeader.getId()).set("opcode", requestHeader.getRawOpcode())
          .set("rd", requestHeader.isRd()).set("z", requestHeader.isZ())
          .set("cd", requestHeader.isCd()).set("qdcount", (int) requestHeader.getQdCount())
          .set("id", requestHeader.getId()).set("q_tc", requestHeader.isTc())
          .set("q_ra", requestHeader.isRa()).set("q_ad", requestHeader.isAd())
          .set("q_rcode", requestHeader.getRawRcode());

      // ip fragments in the request
      if (reqTransport.isFragmented()) {
        int req_frags = reqTransport.getReassembledFragments();
        builder.set("frag", req_frags);

        if ((reqTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && req_frags > 1) {
          metrics.incrementRequestUDPFragmentedCount();
        } else if ((reqTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && req_frags > 1) {
          metrics.incrementRequestTCPFragmentedCount();
        }
      } // end request only section

      // update metrics
      metrics.incrementRequestBytes(reqTransport.getUdpLength());

      if (!combo.isExpired()) {
        // do not send expired queries, this will cause duplicate timestamps with low values
        // this looks like dips in the grafana graph
        metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_QUERY_COUNT, 1, time);
      }
    }

    if (rcode == RCODE_QUERY_WITHOUT_RESPONSE) {
      // no response found for query, update stats
      metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_NO_RESPONSE_COUNT, 1, time);
    }

    // question
    writeQuestion(question, builder);

    // EDNS0 for request
    writeRequestOptions(requestMessage, builder);

    // calculate the processing time
    writeProctime(reqTransport, respTransport, builder);


    if (builder.get("id") == null) {
      LOGGER.error("id is not set for {}", builder);
    } else {
      // create the actual record and write to parquet file
      GenericRecord record = builder.build();
      writer.write(record);
    }

    if (requestHeader != null) {
      metrics.incrementOpcode(requestHeader.getRawOpcode());
    } else if (responseHeader != null) {
      metrics.incrementOpcode(responseHeader.getRawOpcode());
    }

    metrics.incrementRcode(rcode);

    // ip version stats
    updateIpVersionMetrics(reqTransport, respTransport);

    // if packet was expired and dropped from cache then increase stats for this
    if (combo.isExpired()) {
      metrics.incrementExpiredDnsQueryCount();
    }
  }

  private void enrich(Packet reqPacket, Packet respPacket, GenericRecordBuilder builder) {
    String country;
    String ip;

    if (reqPacket != null) {
      // request packet, check the source address
      ip = reqPacket.getSrc();
    } else {
      // response packet, check the destination address
      ip = respPacket.getDst();
    }

    country = getCountry(ip);
    builder.set("country", country);

    // asn = getAsn(ip);

    AutonomousSystem as = geoLookup.lookupAutonomousSystem(ip);
    if (as != null) {
      builder.set("asn_organisation", as.getAutonomousSystemOrganization());
      builder.set("asn", as.getAutonomousSystemNumber());
    }

    boolean foundMatch = false;
    for (String column : subnetChecks.getColumns()) {
      boolean match = !foundMatch && subnetChecks.get(column).isMatch(ip);
      if (match) {
        foundMatch = true;
        metrics.incrementSubnetMatch(column);
      }
      if (descriptor.getSchema().getField(column) != null) {
        LOGGER.debug("Setting field {} => {}", column, match);
        builder.set(column, match);
      } else {
        String msg = "Our schema does not know field [" + column + "] => ignoring";
        if (!errorMessages.contains(msg)) {
          LOGGER.error(msg);
          LOGGER.error("fields: " + descriptor.getSchema().getFields());
          errorMessages.add(msg);
        }
      }
    }
  }

  private void updateIpVersionMetrics(Packet req, Packet resp) {
    if (req != null) {
      if (req.getIpVersion() == 4) {
        metrics.incrementIpv4QueryCount();
      } else {
        metrics.incrementIpv6QueryCount();
      }
    } else {
      if (resp.getIpVersion() == 4) {
        metrics.incrementIpv4QueryCount();
      } else {
        metrics.incrementIpv6QueryCount();
      }
    }
  }

  private void writeQuestion(Question q, GenericRecordBuilder builder) {
    if (q != null) {
      // unassigned, private or unknown, get raw value
      builder.set("qtype", q.getqTypeValue());
      // unassigned, private or unknown, get raw value
      builder.set("qclass", q.getqClassValue());
      // qtype metrics
      metrics.incrementQueryType(q.getqTypeValue());
    }
  }


  // calc the number of seconds between receivinfg the response and sending it back to the resolver
  private void writeProctime(Packet reqTransport, Packet respTransport,
                             GenericRecordBuilder builder) {
    if (reqTransport != null && respTransport != null) {
      Timestamp reqTs = new Timestamp((reqTransport.getTs() * 1000000));
      Timestamp respTs = new Timestamp((respTransport.getTs() * 1000000));

      // from second to microseconds
      long millis1 = respTs.getTime() - reqTs.getTime();
      long millis2 = (respTransport.getTsmicros() - reqTransport.getTsmicros());
      builder.set("proc_time", millis1 + millis2);
    }
  }

  /**
   * Write EDNS0 option (if any are present) to file.
   *
   * @param message --
   * @param builder --
   */
  private void writeResponseOptions(Message message, GenericRecordBuilder builder) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof NSidOption) {
          String id = ((NSidOption) option).getId();
          builder.set("edns_nsid", id != null ? id : "");

          // this is the only server edns data we support, stop processing other options
          break;
        }
      }
    }

  }

  /**
   * Write EDNS0 option (if any are present) to file.
   *
   * @param message the DNS message to analyze
   * @param builder used for adding fields
   */
  private void writeRequestOptions(Message message, GenericRecordBuilder builder) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      builder.set("edns_udp", (int) opt.getUdpPlayloadSize())
          .set("edns_version", (int) opt.getVersion()).set("edns_do", opt.getDnssecDo())
          .set("edns_padding", -1); // use default no padding found

      List<Integer> otherEdnsOptions = new ArrayList<>();
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof PingOption) {
          builder.set("edns_ping", true);
        } else if (option instanceof DNSSECOption) {
          if (option.getCode() == DNSSECOption.OPTION_CODE_DAU) {
            builder.set("edns_dnssec_dau", ((DNSSECOption) option).export());
          } else if (option.getCode() == DNSSECOption.OPTION_CODE_DHU) {
            builder.set("edns_dnssec_dhu", ((DNSSECOption) option).export());
          } else { // N3U
            builder.set("edns_dnssec_n3u", ((DNSSECOption) option).export());
          }
        } else if (option instanceof ClientSubnetOption) {
          ClientSubnetOption scOption = (ClientSubnetOption) option;
          // get client country and asn
          String clientCountry = null;
          String clientASN = null;
          if (scOption.getAddress() != null) {
            InetAddress addr = scOption.getInetAddress();
            try {
              clientCountry = geoLookup.lookupCountry(addr);
              clientASN = geoLookup.lookupASN(addr);
            } catch (Exception e) {
              LOGGER.error("Could not convert IP addr to bytes, invalid address? :" + scOption.getAddress());
            }
          }
          builder.set("edns_client_subnet", scOption.export())
              .set("edns_client_subnet_asn", clientASN)
              .set("edns_client_subnet_country", clientCountry);

        } else if (option instanceof PaddingOption) {
          builder.set("edns_padding", ((PaddingOption) option).getLength());
        } else if (option instanceof KeyTagOption) {
          KeyTagOption kto = (KeyTagOption) option;
          builder.set("edns_keytag_count", kto.getKeytags().size());
          builder.set("edns_keytag_list", Joiner.on(",").join(kto.getKeytags()));
        } else {
          // other
          otherEdnsOptions.add(option.getCode());
        }
      }

      if (otherEdnsOptions.size() > 0) {
        builder.set("edns_other", Joiner.on(",").join(otherEdnsOptions));
      }
    }
  }

  @Override
  protected PartitionStrategy createPartitionStrategy() {
    return new PartitionStrategy.Builder().year("time").month("time").day("time")
        .identity("svr", "server").build();
  }


}

