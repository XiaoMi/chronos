package com.xiaomi.infra.chronos;

import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.generated.ChronosService;
import com.xiaomi.infra.chronos.zookeeper.FailoverServer;
import com.xiaomi.infra.chronos.zookeeper.HostPort;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;
import java.io.IOException;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link ChronosServer}
 */
public class TestChronosServer {

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
      throws IOException, KeeperException, TTransportException, ChronosException {
    ChronosServer chronosServer = createChronosServer(new HostPort("127.0.0.1", 10086));
    ZooKeeperUtil.deleteNodeRecursively(chronosServer.getFailoverWatcher(),
      chronosServer.getFailoverWatcher().getBaseZnode());
    chronosServer.getFailoverWatcher().close();
  }

  public ChronosServer createChronosServer(HostPort hostPort)
      throws IOException, TTransportException, ChronosException {
    Properties properties = new Properties();
    properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
    properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
    properties.setProperty(FailoverServer.BASE_ZNODE, "/chronos/test-cluster");
    properties.setProperty(FailoverServer.ZK_QUORUM, ZK_SERVER.getConnectString());
    properties.setProperty(FailoverServer.SESSION_TIMEOUT, String.valueOf(3000));
    properties.setProperty(ChronosServer.MAX_THREAD, "100000");
    properties.setProperty(ChronosServer.ZK_ADVANCE_TIMESTAMP, "100000");
    properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));

    return new ChronosServer(new ChronosServerWatcher(properties));
  }

  @Test
  public void testDoAsActiveServer() throws TException, IOException, ChronosException {
    final ChronosServer chronosServer = createChronosServer(new HostPort("127.0.0.1", 2187));
    Thread thread = new Thread() {
      @Override
      public void run() {
        chronosServer.doAsActiveServer();
      }
    };
    thread.start();

    TTransport transport = new TSocket("127.0.0.1", 2187);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    ChronosService.Client client = new ChronosService.Client(protocol);
    client.getTimestamp();
    transport.close();

    chronosServer.stopThriftServer();
    assert true;

    chronosServer.getFailoverWatcher().close();
  }

}
