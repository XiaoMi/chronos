package com.xiaomi.infra.chronos;

import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.exception.FatalChronosException;
import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link ChronosImplement}
 */
public class TestChronosImplement {
  protected final static Log LOG = LogFactory.getLog(TestChronosImplement.class);

  private static TestingServer ZK_SERVER;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ZK_SERVER = new TestingServer(true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ZK_SERVER.close();
  }

  @Before
  public void resetZooKeeper()
      throws IOException, KeeperException, FatalChronosException, ChronosException {
    ChronosImplement chronosImplement = createChronosImplement(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(chronosImplement.getChronosServerWatcher(),
      chronosImplement.getChronosServerWatcher().getBaseZnode());
    chronosImplement.getChronosServerWatcher().close();
  }

  private ChronosImplement createChronosImplement(HostPort hostPort)
      throws FatalChronosException, ChronosException, IOException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/chronos/test-cluster");
    properties.setProperty(FailoverServer.ZK_QUORUM, ZK_SERVER.getConnectString());
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosServer.ZK_ADVANCE_TIMESTAMP, "100000");
    properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosImplement(properties, new ChronosServerWatcher(properties));
  }

  @Test
  public void testGetTimestamp()
      throws FatalChronosException, ChronosException, IOException, TException {
    ChronosImplement chronosImplement = createChronosImplement(new HostPort("127.0.0.1", 10086));

    chronosImplement.initTimestamp();
    long firstTimestamp = chronosImplement.getTimestamp();
    long secondTimestamp = chronosImplement.getTimestamp();
    Assert.assertTrue(firstTimestamp < secondTimestamp);

    chronosImplement.getChronosServerWatcher().close();
  }

}
