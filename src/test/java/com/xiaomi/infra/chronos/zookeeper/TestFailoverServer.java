package com.xiaomi.infra.chronos.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.FailoverWatcher;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;

/**
 * Test {@link FailoverServer}
 */
public class TestFailoverServer {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(new Configuration());

  public class TestableFailoverServer extends FailoverServer {
    private final Log LOG = LogFactory.getLog(TestableFailoverServer.class);
    
    private boolean isRunning = false;

    public TestableFailoverServer(FailoverWatcher failoverWatcher) {
      super(failoverWatcher);
    }

    public void doAsActiveServer() {
      isRunning = true;
      try {
        Thread.sleep(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
        LOG.info("sever is interruptted to stop");
        isRunning = false;
      }
      isRunning = false;
    }

    public boolean isRunning() {
      return isRunning;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }
  
  @Before
  public void resetZooKeeper() throws IOException, KeeperException {
    TestableFailoverServer failoverServer = createFailoverServer(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(failoverServer.getFailoverWatcher(), failoverServer
        .getFailoverWatcher().getBaseZnode());
    failoverServer.getFailoverWatcher().close();
  }

  public TestableFailoverServer createFailoverServer(HostPort hostPort) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/test-failover");
    properties.setProperty(FailoverServer.ZK_QUORUM, ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));

    FailoverWatcher failoverWatcher = new FailoverWatcher(properties);
    return new TestableFailoverServer(failoverWatcher);
  }

  @Test
  public void testRun() throws KeeperException, InterruptedException, IOException {
    final TestableFailoverServer server1 = createFailoverServer(new HostPort("127.0.0.1", 11111));
    Thread thread1 = new Thread() {
      @Override
      public void run() {
        server1.run();
      }
    };
    thread1.start();

    final TestableFailoverServer server2 = createFailoverServer(new HostPort("127.0.0.1", 22222));
    Thread thread2 = new Thread() {
      @Override
      public void run() {
        server2.run();
      }
    };
    thread2.start();

    assertTrue(server1.isRunning());
    assertFalse(server2.isRunning());

    // stop server1 and server2 can run as active server
    thread1.interrupt();
    ZooKeeperUtil.deleteNode(server1.getFailoverWatcher(), server1.getFailoverWatcher().getMasterZnode());

    Thread.sleep(500); // wait for server2 to become active server
    assertFalse(server1.isRunning());
    assertTrue(server2.isRunning());
    thread2.interrupt();
    
    server1.getFailoverWatcher().close();
    server2.getFailoverWatcher().close();
  }

}
