package com.xiaomi.infra.chronos.zookeeper;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.xiaomi.infra.chronos.zookeeper.HostPort;

/**
 * Test {@link HostPort}
 */
public class TestHostPort {

  @Test
  public void testGetHost() {
    HostPort hostPort = new HostPort("127.0.0.1", 2181);
    assertTrue(hostPort.getHost().equals("127.0.0.1"));
  }

  @Test
  public void testGetPort() {
    HostPort hostPort = new HostPort("127.0.0.1", 2182);
    assertTrue(hostPort.getPort() == 2182);
  }

  @Test
  public void testGetHostPort() {
    HostPort hostPort = new HostPort("127.0.0.1", 2183);
    assertTrue(hostPort.getHostPort().equals("127.0.0.1_2183"));
  }

  @Test
  public void testParseHostPort() {
    String string = "127.0.0.1_2184";
    HostPort hostPort = HostPort.parseHostPort(string);
    assertTrue(hostPort.getHost().equals("127.0.0.1"));
    assertTrue(hostPort.getPort() == 2184);
  }

}
