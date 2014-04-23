package com.xiaomi.infra.chronos.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.zookeeper.KeeperException;

import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.FailoverWatcher;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;

/**
 * Test {@link FailoverWatcher}
 */
public class TestFailoverWatcher {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(new Configuration());

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
    FailoverWatcher failoverWatcher = createFailoverWatcher(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(failoverWatcher, failoverWatcher.getBaseZnode());
    failoverWatcher.close();
  }

  private FailoverWatcher createFailoverWatcher(HostPort hostPort) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/test-failover");
    properties.setProperty(FailoverServer.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new FailoverWatcher(properties);
  }

  @Test
  public void testInitZnode() throws IOException, KeeperException {
    FailoverWatcher failoverWatcher = createFailoverWatcher(new HostPort("127.0.0.1", 10086));

    assertTrue(ZooKeeperUtil.watchAndCheckExists(failoverWatcher, failoverWatcher.getBaseZnode()));
    assertTrue(ZooKeeperUtil.watchAndCheckExists(failoverWatcher, failoverWatcher.getBackupServersZnode()));
    
    failoverWatcher.close();
  }

  @Test
  public void testBlockUntilActive() throws IOException, KeeperException, InterruptedException {
    // the first server, expect to be active master
    HostPort hostPort1 = new HostPort("127.0.0.1", 11111);
    FailoverWatcher failoverWatcher1 = createFailoverWatcher(hostPort1);
    failoverWatcher1.blockUntilActive();

    assertTrue(failoverWatcher1.hasActiveServer());
    String activeServer = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher1,
      failoverWatcher1.masterZnode));
    assertTrue(activeServer.equals(hostPort1.getHostPort()));

    // the second server, expect to be backup master
    HostPort hostPort2 = new HostPort("127.0.0.1", 22222);
    final FailoverWatcher failoverWatcher2 = createFailoverWatcher(hostPort2);
    Thread thread2 = new Thread() {
      @Override
      public void run() {
        failoverWatcher2.blockUntilActive();
      }
    };
    thread2.start();

    Thread.sleep(500); // wait for second watcher to create znode
    List<String> backupMastersList = ZooKeeperUtil.listChildrenAndWatchForNewChildren(
      failoverWatcher1, failoverWatcher1.backupServersZnode);
    assertTrue(failoverWatcher2.hasActiveServer());
    assertTrue(backupMastersList.contains(failoverWatcher2.getHostPort().getHostPort()));

    thread2.interrupt();
    failoverWatcher1.close();
    failoverWatcher2.close();
  }

  @Test
  public void testRestartActiveMaster() throws IOException, KeeperException {
    // the first server, expect to be active master
    HostPort hostPort1 = new HostPort("127.0.0.1", 11111);
    FailoverWatcher failoverWatcher1 = createFailoverWatcher(hostPort1);
    failoverWatcher1.blockUntilActive();

    assertTrue(failoverWatcher1.hasActiveServer());
    String activeServer = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher1,
      failoverWatcher1.masterZnode));
    assertTrue(activeServer.equals(hostPort1.getHostPort()));

    // the same server start, expect to restart active master
    FailoverWatcher failoverWatcher2 = createFailoverWatcher(hostPort1);
    failoverWatcher2.blockUntilActive();
    assertTrue(failoverWatcher2.hasActiveServer());
    String activeServer2 = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher1,
      failoverWatcher1.masterZnode));
    assertTrue(activeServer2.equals(hostPort1.getHostPort()));
    
    failoverWatcher1.close();
    failoverWatcher2.close();
  }

  @Test
  public void testHandleMasterNodeChange() throws IOException, KeeperException,
      InterruptedException {
    HostPort hostPort1 = new HostPort("127.0.0.1", 11111);
    FailoverWatcher failoverWatcher1 = createFailoverWatcher(hostPort1);
    failoverWatcher1.blockUntilActive();

    // hostPort2 is backup master
    HostPort hostPort2 = new HostPort("127.0.0.1", 22222);
    final FailoverWatcher failoverWatcher2 = createFailoverWatcher(hostPort2);
    Thread thread2 = new Thread() {
      @Override
      public void run() {
        failoverWatcher2.blockUntilActive();
      }
    };
    thread2.start();

    // hostPort3 is backup master
    HostPort hostPort3 = new HostPort("127.0.0.1", 33333);
    final FailoverWatcher failoverWatcher3 = createFailoverWatcher(hostPort3);
    Thread thread3 = new Thread() {
      @Override
      public void run() {
        failoverWatcher3.blockUntilActive();
      }
    };
    thread3.start();

    // delete the master node, then one of them will become master, the other will be backup master
    ZooKeeperUtil.deleteNode(failoverWatcher1, failoverWatcher1.masterZnode);
    Thread.sleep(500); // wait for leader election
    String masterZnodeData = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher1,
      failoverWatcher1.masterZnode));
    String backupMasterNode = ZooKeeperUtil.listChildrenAndWatchForNewChildren(failoverWatcher1,
      failoverWatcher1.backupServersZnode).get(0);

    if (masterZnodeData.equals(hostPort2.getHostPort())) {
      if (!backupMasterNode.equals(hostPort3.getHostPort())) {
        assert false;
      }
    } else if (masterZnodeData.equals(hostPort3.getHostPort())) {
      if (!backupMasterNode.equals(hostPort2.getHostPort())) {
        assert false;
      }
    } else {
      assert false;
    }

    if (thread2.isAlive()) {
      thread2.interrupt();
    } else {
      thread3.interrupt();
    }
    
    failoverWatcher1.close();
    failoverWatcher2.close();
  }

  @Test
  public void testCloseMaster() throws KeeperException, IOException {
    HostPort hostPort1 = new HostPort("127.0.0.1", 11111);
    FailoverWatcher failoverWatcher1 = createFailoverWatcher(hostPort1);
    failoverWatcher1.blockUntilActive();

    HostPort hostPort2 = new HostPort("127.0.0.1", 22222);
    final FailoverWatcher failoverWatcher2 = createFailoverWatcher(hostPort2);
    Thread thread2 = new Thread() {
      @Override
      public void run() {
        failoverWatcher2.blockUntilActive();
      }
    };
    thread2.start();

    // delete master znode, then failoverWatcher2 will be master
    ZooKeeperUtil.deleteNode(failoverWatcher2, failoverWatcher2.getMasterZnode());
    String masterZnodeData = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher2,
      failoverWatcher2.masterZnode));
    List<String> backupMasterList = ZooKeeperUtil.listChildrenAndWatchForNewChildren(
      failoverWatcher2, failoverWatcher2.backupServersZnode);
    assertTrue(masterZnodeData.equals(failoverWatcher2.getHostPort().getHostPort()));
    assertTrue(backupMasterList.size() == 0);
    
    failoverWatcher1.close();
    failoverWatcher2.close();
  }

  @Test
  public void testCloseBackupMaster() throws IOException, KeeperException, InterruptedException {
    HostPort hostPort1 = new HostPort("127.0.0.1", 11111);
    FailoverWatcher failoverWatcher1 = createFailoverWatcher(hostPort1);
    failoverWatcher1.blockUntilActive();

    HostPort hostPort2 = new HostPort("127.0.0.1", 22222);
    final FailoverWatcher failoverWatcher2 = createFailoverWatcher(hostPort2);
    Thread thread2 = new Thread() {
      @Override
      public void run() {
        failoverWatcher2.blockUntilActive();
      }
    };
    thread2.start();

    Thread.sleep(500); // wait for backup master to init
    thread2.interrupt(); // close backup master and failoverWatcher1 is still active master
    ZooKeeperUtil.deleteNode(failoverWatcher1, failoverWatcher1.getBackupServersZnode() + "/"
        + hostPort2.getHostPort());

    String masterZnodeData = new String(ZooKeeperUtil.getDataAndWatch(failoverWatcher1,
      failoverWatcher1.masterZnode));
    List<String> backupMasterList = ZooKeeperUtil.listChildrenAndWatchForNewChildren(
      failoverWatcher1, failoverWatcher1.backupServersZnode);
    assertTrue(masterZnodeData.equals(failoverWatcher1.getHostPort().getHostPort()));
    assertTrue(backupMasterList.size() == 0);

    failoverWatcher1.close();
    failoverWatcher2.close();
  }

}
