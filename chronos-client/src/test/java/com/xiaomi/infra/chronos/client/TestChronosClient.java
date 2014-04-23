package com.xiaomi.infra.chronos.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.chronos.client.ChronosClient;
import com.xiaomi.infra.chronos.client.ChronosClientWatcher;
import com.xiaomi.infra.chronos.ChronosServer;
import com.xiaomi.infra.chronos.ChronosServerWatcher;
import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.HostPort;

/**
 * Test {@link ChronosClient}
 * 
 * Pass with thrift-0.8.0, but not pass with thrift-0.5.0 because the class of server is not found.
 */
public class TestChronosClient {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(new Configuration());

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  public ChronosClient createChronosServer() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ChronosClient.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(ChronosClient.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosClient.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosClient(new ChronosClientWatcher(properties));
  }

  public ChronosServer createChronosServer(HostPort hostPort) throws IOException, TTransportException, ChronosException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/chronos/default-cluster");
    properties.setProperty(FailoverServer.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()));
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosServer.MAX_THREAD, "100000");
    properties.setProperty(ChronosServer.ZK_ADVANCE_TIMESTAMP, "100000");
    properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));

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
    ChronosClient chronosClient = createChronosServer();
    long timestamp1 = chronosClient.getTimestamp();
    long timestamp2 = chronosClient.getTimestamp();
    assertTrue(timestamp1 < timestamp2);
    
    chronosServer.stopThriftServer();
    chronosServer.getFailoverWatcher().close();
    chronosClient.getChronosClientWatcher().close();
  }
  
  @Test
  public void testGetTimestamps() throws IOException, InterruptedException, TTransportException, ChronosException {
    final ChronosServer chronosServer = createChronosServer(new HostPort("127.0.0.1", 2188));
    Thread thread = new Thread() {
      @Override
      public void run() {
        chronosServer.run();
      }
    };
    thread.start();

    Thread.sleep(500); // wait for server to start
    ChronosClient chronosClient = createChronosServer();
    long timestamp1 = chronosClient.getTimestamps(100);
    long timestamp2 = chronosClient.getTimestamps(100);
    assertTrue(timestamp2 - timestamp1 >= 100);

    chronosServer.stopThriftServer();
    chronosServer.getFailoverWatcher().close();
    chronosClient.getChronosClientWatcher().close();
  }

}
