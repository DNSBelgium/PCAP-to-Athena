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

package be.dnsbelgium.data.pcap.utils;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */

public class GeoLookupUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoLookupUtil.class);

  private static final String MAXMIND_COUNTRY_DB = "GeoLite2-Country.mmdb";
  private static final String MAXMIND_ASN_DB = "GeoLite2-ASN.mmdb";

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  public GeoLookupUtil(String folder) {
    LOGGER.info("Loading Maxmind GEO/ASN database in folder {}", folder);
    try {
      // geo
      File database = new File(folder, MAXMIND_COUNTRY_DB);
      geoReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
      // asn
      database = new File(folder, MAXMIND_ASN_DB);
      asnReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();

      LOGGER.info("loaded geo database of {}", geoReader.getMetadata().getBuildDate());
      LOGGER.info("loaded geo database type {}", geoReader.getMetadata().getDatabaseType());
      LOGGER.info("loaded ASN database of {}", asnReader.getMetadata().getBuildDate());

    } catch (IOException e) {
      LOGGER.error("You probaly need to run scripts/download_maxmind_geo_ip_db.sh");
      throw new RuntimeException("Error initializing Maxmind GEO/ASN database from " + folder, e);
    }
  }

  public String lookupCountry(String ip) {
    try {
      return lookupCountry(InetAddress.getByName(ip));
    } catch (UnknownHostException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No country found for: " + ip);
      }
      return null;
    }
  }

  public String lookupCountry(InetAddress addr) {
    CountryResponse response;

    try {
      response = geoReader.country(addr);
    } catch (Exception e) {
      LOGGER.debug("No country found for {}", addr);
      return null;
    }
    return response.getCountry().getIsoCode();
  }

  public String lookupASN(InetAddress ip) {
    try {
      AsnResponse ar = asnReader.asn(ip);
      return String.valueOf(ar.getAutonomousSystemNumber());
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No asn found for: " + ip);
      }
    }
    return null;
  }

  public String lookupASN(String ip) {
    InetAddress inetAddr;
    try {
      inetAddr = InetAddress.getByName(ip);
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Invalid IP address: " + ip);
      }
      return null;
    }
    return lookupASN(inetAddr);
  }

  public AutonomousSystem lookupAutonomousSystem(String ip) {
    try {
      InetAddress inetAddr = InetAddress.getByName(ip);
      AsnResponse asnResponse = asnReader.asn(inetAddr);
      return new AutonomousSystem(asnResponse);
    } catch (GeoIp2Exception | IOException e) {
      LOGGER.debug("No ASN found for {} : {}", ip, e.getMessage());
      return null;
    }
  }


}
