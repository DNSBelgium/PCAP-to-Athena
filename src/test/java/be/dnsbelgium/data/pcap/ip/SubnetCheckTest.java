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

package be.dnsbelgium.data.pcap.ip;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SubnetCheckTest {

  private List<String> subnets = Lists.newArrayList("10.10.10.10/25", "5.6.7.8/32");
  private SubnetFetcher fetcher;

  @Before
  public void before() {
    fetcher = mock(SubnetFetcher.class);
  }

  @Test
  public void fileDoesNotExist() throws IOException {
    when(fetcher.fetchSubnets()).thenReturn(subnets);
    File tempFile = File.createTempFile("SubnetCheckTest", ".txt");
    SubnetCheck check = new SubnetCheck(tempFile, fetcher);
    assertEquals(0, check.getSubnetCount());
    check.init();
    verify(fetcher).fetchSubnets();
    assertTrue("file should now exist", tempFile.exists());
    assertEquals(2, check.getSubnetCount());
    assertMatches(check);
  }

  @Test
  public void fileAlreadyExists() throws IOException {
    File tempFile = File.createTempFile("SubnetCheckTest", ".txt");
    Files.write(tempFile.toPath(), subnets, TRUNCATE_EXISTING);
    SubnetCheck check = new SubnetCheck(tempFile, fetcher);
    assertEquals(0, check.getSubnetCount());
    check.init();
    verify(fetcher, never()).fetchSubnets();
    assertEquals(2, check.getSubnetCount());
    assertMatches(check);
  }

  @Test
  public void fetchingFails() throws IOException {
    List<String> newData = Lists.newArrayList("2.3.4.5/32");
    when(fetcher.fetchSubnets())
        .thenReturn(subnets)
        .thenThrow(new IOException("The internet is crazy"))
        .thenReturn(newData);

    File tempFile = File.createTempFile("SubnetCheckTest", ".txt");
    SubnetCheck check = new SubnetCheck(tempFile, fetcher);
    assertEquals(0, check.getSubnetCount());
    check.init();
    verify(fetcher).fetchSubnets();
    assertTrue("file should now exist", tempFile.exists());
    assertEquals(2, check.getSubnetCount());
    assertMatches(check);
    assertTrue(tempFile.setLastModified(DateTime.now().minusHours(30).getMillis()));
    check.update();
    assertEquals(2, check.getSubnetCount());
    assertMatches(check);
    check.update();
    assertEquals(1, check.getSubnetCount());
    assertFalse(check.isMatch("10.10.10.10"));
    assertTrue(check.isMatch("2.3.4.5"));
  }

  private void assertMatches(SubnetCheck check) {
    assertTrue(check.isMatch("10.10.10.10"));
    assertTrue(check.isMatch("10.10.10.11"));
    assertTrue(check.isMatch("5.6.7.8"));
    assertFalse(check.isMatch("5.6.7.9"));
  }

}