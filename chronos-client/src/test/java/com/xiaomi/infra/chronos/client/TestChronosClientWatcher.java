package com.xiaomi.infra.chronos.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.chronos.client.ChronosClientWatcher;
import com.xiaomi.infra.chronos.ChronosServer;
import com.xiaomi.infra.chronos.ChronosServerWatcher;
import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;

/**
 * Test {@link ChronosClientWatcher}
 * 
 * Pass with thrift-0.8.0, but not pass with thrift-0.5.0 because the class of server is not found.
 */
public class TestChronosClientWatcher {
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
  public void resetZooKeeper() throws IOException, KeeperException, TTransportException,
      ChronosException {
    ChronosServer chronosServer = createChronosServer(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(chronosServer.getFailoverWatcher(), chronosServer
        .getFailoverWatcher().getBaseZnode());
    chronosServer.getFailoverWatcher().close();
  }

  private ChronosClientWatcher createChronosClientWatcher() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ChronosClient.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(ChronosClient.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosClient.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosClientWatcher(properties);
  }

  public ChronosServer createChronosServer(HostPort hostPort) throws IOException, TTransportException, ChronosException {
    Properties properties = new Properties();
    properties.setProperty(ChronosServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(ChronosServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(ChronosServer.BASE_ZNODE, "/chronos/default-cluster");
    properties.setProperty(ChronosServer.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(ChronosServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosServer.MAX_THREAD, "100000");
    properties.setProperty(ChronosServer.ZK_ADVANCE_TIMESTAMP, "100000");
    properties.setProperty(ChronosServer.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosServer(new ChronosServerWatcher(properties));
  }

  @Test
  public void testGetTimestamp() throws IOException, InterruptedException, TTransportException, ChronosException {
    final ChronosServer chronosServer = createChronosServer(new HostPort("127.0.0.1", 2187));
    Thread thread = new Thread() {
      @Override
      public void run() {
        chronosServer.run();
      }
    };
    thread.start();

    Thread.sleep(500); // wait for server to start
    ChronosClientWatcher chronosClientWatcher = createChronosClientWatcher();
    chronosClientWatcher.getTimestamp();
    
    chronosServer.stopThriftServer();
    chronosServer.getFailoverWatcher().close();
    chronosClientWatcher.close();
    assertTrue(true);
  }

}
