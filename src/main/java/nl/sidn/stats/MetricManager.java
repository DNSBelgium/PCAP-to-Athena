package nl.sidn.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"unused", "JavaDoc"})
public class MetricManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricManager.class);

  //dns stats
  public static String METRIC_IMPORT_DNS_QUERY_COUNT = ".dns.request.count";
  public static String METRIC_IMPORT_DNS_RESPONSE_COUNT = ".dns.response.count";
  public static String METRIC_IMPORT_DNS_NO_REQUEST_COUNT = ".dns.response.norequest.count";
  public static String METRIC_IMPORT_DNS_QTYPE = ".dns.request.qtype";
  public static String METRIC_IMPORT_DNS_RCODE = ".dns.request.rcode";
  public static String METRIC_IMPORT_DNS_OPCODE = ".dns.request.opcode";
  public static String METRIC_IMPORT_DNS_NO_RESPONSE_COUNT = ".dns.noreresponse.count";

  //layer 4 stats
  public static String METRIC_IMPORT_DNS_TCPSTREAM_COUNT = ".dns.tcp.session.count";
  public static String METRIC_IMPORT_TCP_COUNT = ".tcp.packet.count";
  public static String METRIC_IMPORT_UDP_COUNT = ".udp.packet.count";

  public static String METRIC_IMPORT_UDP_REQUEST_FRAGMENTED_COUNT = ".udp.request.fragmented.count";
  public static String METRIC_IMPORT_UDP_RESPONSE_FRAGMENTED_COUNT = ".udp.response.fragmented.count";
  public static String METRIC_IMPORT_TCP_REQUEST_FRAGMENTED_COUNT = ".tcp.request.fragmented.count";
  public static String METRIC_IMPORT_TCP_RESPONSE_FRAGMENTED_COUNT = ".tcp.response.fragmented.count";
  public static String METRIC_IMPORT_IP_VERSION_4_COUNT = ".ip.version.4.count";
  public static String METRIC_IMPORT_IP_VERSION_6_COUNT = ".ip.version.6.count";

  public static String METRIC_IMPORT_IP_COUNT = ".ip.count";
  public static String METRIC_IMPORT_COUNTRY_COUNT = ".country.count";
  public static String METRIC_IMPORT_ASN_COUNT = ".asn.count";
  public static String METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE = ".dns.response.bytes.size";
  public static String METRIC_IMPORT_DNS_QUERY_BYTES_SIZE = ".dns.request.bytes.size";

  //decoder app stats
  public static String METRIC_IMPORT_DNS_COUNT = ".dns.message.count";
  public static String METRIC_IMPORT_FILES_COUNT = ".files.count";
  public static String METRIC_IMPORT_RUN_TIME = ".time.duration";
  public static String METRIC_IMPORT_RUN_ERROR_COUNT = ".run.error.count";

  public static String METRIC_IMPORT_TCP_PREFIX_ERROR_COUNT = ".tcp.prefix.error.count";
  public static String METRIC_IMPORT_DNS_DECODE_ERROR_COUNT = ".dns.decode.error.count";

  public static String METRIC_IMPORT_STATE_PERSIST_UDP_FLOW_COUNT = ".state.persist.udp.flow.count";
  public static String METRIC_IMPORT_STATE_PERSIST_TCP_FLOW_COUNT = ".state.persist.tcp.flow.count";
  public static String METRIC_IMPORT_STATE_PERSIST_DNS_COUNT = ".state.persist.dns.count";

  //icmp
  public static String METRIC_ICMP_COUNT = ".icmp.packet.count";
  public static String METRIC_ICMP_V4 = ".icmp.v4";
  public static String METRIC_ICMP_V6 = ".icmp.v6";
  public static String METRIC_ICMP_PREFIX_TYPE_V4 = ".icmp.v4.prefix.type";
  public static String METRIC_ICMP_PREFIX_TYPE_V6 = ".icmp.v6.prefix.type";
  public static String METRIC_ICMP_ERROR = ".icmp.error";
  public static String METRIC_ICMP_INFO = ".icmp.info";

  //cache stats
  public static String METRIC_IMPORT_CACHE_EXPPIRED_DNS_QUERY_COUNT = ".cache.expired.dns.request.count";

  private static MetricManager _metricManager = null;

  public static MetricManager getInstance(){
    if(_metricManager == null){
      _metricManager = new MetricManager();
    }
    return _metricManager;
  }

  private MetricManager(){
  }

  /**
   * send overall metrics, use current system time
   * @param metric
   * @param value
   */
  public void send(String metric, int value){
    // TODO
  }

  private String createMetricName(String metric){
    // TODO
    return null;
  }


  public void sendAggregated(String metric, int value, long timestamp, boolean useThreshHold){
    // TODO
  }

  /**
   * send aggregated counts (per server) aggregate by 10s bucket
   * @param metric
   * @param value
   * @param timestamp
   */
  public void sendAggregated(String metric, int value, long timestamp){
    sendAggregated(metric, value, timestamp, true);
  }

  public void flush(){
    // TODO
  }


}
