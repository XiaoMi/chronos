package com.xiaomi.infra.chronos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import com.xiaomi.infra.chronos.exception.FatalChronosException;
import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.generated.ChronosService;
import com.xiaomi.infra.chronos.zookeeper.FailoverServer;

/**
 * The thrift server for clients to get the precise auto-increasing timestamp. It relies on
 * ZooKeeper for active/backup servers switching and the max allocated timestamp is persistent in
 * ZooKeeper.
 * 
 * @see ChronosServerWatcher
 */
public class ChronosServer extends FailoverServer {
  private static final Log LOG = LogFactory.getLog(ChronosServer.class);

  public static final String CLUSTER_NAME = "clusterName";
  public static final String MAX_THREAD = "maxThread";
  public static final String ZK_ADVANCE_TIMESTAMP = "zkAdvanceTimestamp";
  public static final String SOCKET_TIMEOUT = "socketTimeout";
  public static final String MESSAGE_LENGTH_LIMIT = "messageLengthLimit";

  private final Properties properties;
  private final ChronosServerWatcher chronosServerWatcher;
  private ChronosImplement chronosImplement;
  private TServer thriftServer;

  /**
   * Construct ChronosServer with ChronosServerWatcher and properties.
   *
   * @param chronosServerWatcher the ChronosServerWatcher
   * @param properties the properties of ChronosServer
   * @throws TTransportException when error to init thrift server
   * @throws ChronosException when error to construct ChronosServerWatcher
   */
  public ChronosServer(final ChronosServerWatcher chronosServerWatcher, Properties properties)
      throws TTransportException, ChronosException {
    super(chronosServerWatcher);
    this.chronosServerWatcher = chronosServerWatcher;
    this.properties = properties;

    LOG.info("Init thrift server in " + chronosServerWatcher.getHostPort().getHostPort());
    initThriftServer();
  }

  /**
   * Construct ChronosServer with ChronosServerWatcher.
   *
   * @throws TTransportException when error to init thrift server
   * @throws ChronosException when error to construct ChronosServerWatcher
   */
  public ChronosServer(final ChronosServerWatcher chronosServerWatcher) throws TTransportException,
      ChronosException {
    this(chronosServerWatcher, chronosServerWatcher.getProperties());
  }

  /**
   * Construct ChronosServer with properties.
   *
   * @param properties the properties of ChronosServer
   * @throws TTransportException when error to init thrift server
   * @throws ChronosException when error to construct ChronosServerWatcher
   * @throws IOException when whe error to construct ChronosServerWatcher
   */
  public ChronosServer(Properties properties) throws TTransportException, ChronosException, IOException {
    this(new ChronosServerWatcher(properties), properties);
  }

  /**
   * Initialize Thrift server of ChronosServer.
   *
   * @throws TTransportException when error to initialize thrift server
   * @throws FatalChronosException when set a smaller timestamp in ZooKeeper
   * @throws ChronosException when error to set timestamp in ZooKeeper
   */
  private void initThriftServer() throws TTransportException, FatalChronosException,
      ChronosException {

    int maxThread = Integer.parseInt(properties.getProperty(MAX_THREAD,
      String.valueOf(Integer.MAX_VALUE)));
    String serverHost = properties.getProperty(FailoverServer.SERVER_HOST);
    int serverPort = Integer.parseInt(properties.getProperty(FailoverServer.SERVER_PORT));
    int socketTimeout = Integer
        .parseInt(properties.getProperty(SOCKET_TIMEOUT, String.valueOf(3000L)));
    TServerSocket serverTransport =
        new TServerSocket(new InetSocketAddress(serverHost, serverPort), socketTimeout);
    // Default message length limit is 10KB
    int messageLengthLimit =
        Integer.parseInt(properties.getProperty(MESSAGE_LENGTH_LIMIT, String.valueOf(10240)));
    Factory proFactory = new TBinaryProtocol.Factory(true, false, messageLengthLimit, -1L);

    chronosImplement = new ChronosImplement(properties, chronosServerWatcher);

    TProcessor processor = new ChronosService.Processor(chronosImplement);
    Args rpcArgs = new Args(serverTransport);
    rpcArgs.processor(processor);
    rpcArgs.protocolFactory(proFactory);
    rpcArgs.maxWorkerThreads(maxThread);
    thriftServer = new TThreadPoolServer(rpcArgs);
  }

  /**
   * Initialize persistent timestamp and start to serve as active master.
   */
  @Override
  public void doAsActiveServer() {
    try {
      LOG.info("As active master, init timestamp from ZooKeeper");
      chronosImplement.initTimestamp();
    } catch (Exception e) {
      LOG.fatal("Exception to init timestamp from ZooKeeper, exit immediately");
      System.exit(0);
    }
    
    chronosServerWatcher.setBeenActiveMaster(true);

    LOG.info("Start to accept thrift request");
    startThriftServer();
  }

  /**
   * Stop thrift server of ChronosServer.
   */
  public void stopThriftServer() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
  }

  /**
   * Start thrift server of ChronosServer.
   */
  public void startThriftServer() {
    if (thriftServer != null) {
      thriftServer.serve();
    }
  }

  /**
   * The main entrance of ChronosServer to start the service.
   *
   * @param args will ignore all the command-line arguments
   */
  public static void main(String[] args) {
    Properties properties = new Properties();
    LOG.info("Load chronos.cfg configuration from class path");
    try {
      properties.load(ChronosServer.class.getClassLoader().getResourceAsStream("chronos.cfg"));
      properties.setProperty(FailoverServer.BASE_ZNODE,
        "/chronos" + "/" + properties.getProperty(CLUSTER_NAME));
    } catch (IOException e) {
      LOG.fatal("Error to load chronos.cfg configuration, exit immediately", e);
      System.exit(0);
    }

    ChronosServer chronosServer = null;
    LOG.info("Init chronos server and connect ZooKeeper");
    try {
      chronosServer = new ChronosServer(properties);
    } catch (Exception e) {
      LOG.fatal("Exception to init chronos server, exit immediately", e);
      System.exit(0);
    }
    chronosServer.run();
  }

}
