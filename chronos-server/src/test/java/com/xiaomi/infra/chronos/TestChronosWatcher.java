package com.xiaomi.infra.chronos;

import static org.junit.Assert.assertTrue;

import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.exception.FatalChronosException;
import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;
import java.io.IOException;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link ChronosServerWatcher}
 */
public class TestChronosWatcher {

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
  public void resetZooKeeper() throws IOException, KeeperException {
    ChronosServerWatcher chronosWatcher = createChronosWatcher(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(chronosWatcher, chronosWatcher.getBaseZnode());
    chronosWatcher.close();
  }

  private ChronosServerWatcher createChronosWatcher(HostPort hostPort) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/chronos/test-cluster");
    properties.setProperty(FailoverServer.ZK_QUORUM, ZK_SERVER.getConnectString());
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosServerWatcher(properties);
  }

  @Test
  public void testInitZnode() throws IOException, KeeperException {
    ChronosServerWatcher chronosServerWatcher =
      createChronosWatcher(new HostPort("127.0.0.1", 10086));

    assertTrue(
      ZooKeeperUtil.watchAndCheckExists(chronosServerWatcher, chronosServerWatcher.getBaseZnode()));
    assertTrue(ZooKeeperUtil.watchAndCheckExists(chronosServerWatcher,
      chronosServerWatcher.getBackupServersZnode()));
    assertTrue(ZooKeeperUtil.watchAndCheckExists(chronosServerWatcher,
      chronosServerWatcher.getPersistentTimestampZnode()));

    chronosServerWatcher.close();
  }

  @Test
  public void testSetPersistentTimestamp()
      throws IOException, FatalChronosException, ChronosException, KeeperException {
    ChronosServerWatcher chronosServerWatcher =
      createChronosWatcher(new HostPort("127.0.0.1", 10086));

    assertTrue(chronosServerWatcher.getPersistentTimestamp() == 0);

    long expectTimestamp = System.currentTimeMillis();
    chronosServerWatcher.setPersistentTimestamp(expectTimestamp);
    long actualTimestamp = ZooKeeperUtil.bytesToLong(ZooKeeperUtil
      .getDataAndWatch(chronosServerWatcher, chronosServerWatcher.getPersistentTimestampZnode()));
    assertTrue(actualTimestamp == expectTimestamp);

    chronosServerWatcher.close();
  }

  @Test
  public void testGetPersistentTimestamp() throws IOException, ChronosException, KeeperException {
    ChronosServerWatcher chronosServerWatcher =
      createChronosWatcher(new HostPort("127.0.0.1", 10086));

    assertTrue(chronosServerWatcher.getPersistentTimestamp() == 0);

    long expectTimestamp = System.currentTimeMillis();
    chronosServerWatcher.setPersistentTimestamp(expectTimestamp);
    assertTrue(chronosServerWatcher.getPersistentTimestamp() == expectTimestamp);

    chronosServerWatcher.close();
  }

}
