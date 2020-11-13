package com.xiaomi.infra.chronos.client;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.xiaomi.infra.chronos.client.ChronosClientWatcher;
import com.xiaomi.infra.chronos.generated.ChronosService;

/**
 * ChronosClientWatcher listens to the change of master znode and always connect with the active
 * chronos server. 
 */
public class ChronosClientWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ChronosClientWatcher.class);

  private final String zkQuorum;
  private final String baseZnode;
  private final String masterZnode;
  private final int sessionTimeout;
  private final int connectRetryTimes;
  private final int socketTimeout;
  
  private ZooKeeper zooKeeper;
  private TTransport transport;
  private TProtocol protocol;
  private ChronosService.Client client;

  /**
   * Construct ChronosClientWatcher with properties.
   * 
   * @param properties the properties of ChronosClientWatcher
   * @throws IOException when error to connect ZooKeeper or ChronosServer
   */
  public ChronosClientWatcher(Properties properties) throws IOException {
    zkQuorum = properties.getProperty(ChronosClient.ZK_QUORUM, "127.0.0.1:2181");
    baseZnode = "/chronos/" + properties.getProperty(ChronosClient.CLUSTER_NAME, "default-cluster");
    masterZnode = baseZnode + "/master";
    sessionTimeout = Integer.parseInt(properties.getProperty(ChronosClient.SESSION_TIMEOUT, "5000"));
    connectRetryTimes = Integer.parseInt(properties.getProperty(ChronosClient.CONNECT_RETRY_TIMES, "10"));
    socketTimeout = Integer.parseInt(properties.getProperty(ChronosClient.SOCKET_TIMEOUT, "3000"));
    
    connectZooKeeper();
    connectChronosServer();
  }

  /**
   * Initialize ZooKeeper object and connect with ZooKeeper with retries.
   * 
   * @throws IOException when error to connect with ZooKeeper after retrying
   */
  private void connectZooKeeper() throws IOException {  
    for (int i = 0; i <= connectRetryTimes; i++) {
      try {
        zooKeeper = new ZooKeeper(zkQuorum, sessionTimeout, this);
        LOG.info("Connected ZooKeeper " + zkQuorum);
        break;
      } catch (IOException e) {
        if (i == connectRetryTimes) {
          throw new IOException("Can't connect ZooKeeper after retrying", e);
        }
        LOG.info("Exception to connect ZooKeeper, retry " + (i + 1) + " times");
      }
    }
  }

  /**
   * Reconnect with ZooKeeper.
   * 
   * @throws InterruptedException when interrupt close ZooKeeper object
   * @throws IOException when error to connect with ZooKeeper
   */
  private void reconnectZooKeeper() throws InterruptedException, IOException {
    LOG.info("Try to reconnect ZooKeeper " + zkQuorum);
    
    if (zooKeeper != null) {
      zooKeeper.close();
    }
    connectZooKeeper();
  }
  
  /**
   * Access ZooKeeper to get current master ChronosServer and connect with it.
   * 
   * @throws IOException when error to access ZooKeeper or connect with ChronosServer
   */
  private void connectChronosServer() throws IOException {
    LOG.info("Try to connect chronos server");
    
    byte[] hostPortBytes = getData(this, masterZnode);

    if (hostPortBytes != null) {
      String hostPort = new String(hostPortBytes); // e.g. 127.0.0.0_2181
      LOG.info("Find the active chronos server in " + hostPort);
      try {
        transport = new TSocket(hostPort.split("_")[0], Integer.parseInt(hostPort.split("_")[1]),
            socketTimeout);
        transport.open();
        protocol = new TBinaryProtocol(transport);
        client = new ChronosService.Client(protocol);
      } catch (TException e) {
        throw new IOException("Exception to connect chronos server in " + hostPort);
      }
    } else {
      throw new IOException("The data of " + masterZnode + " is null");
    }
  }

  /**
   * Reconnect with ChronosServer.
   * 
   * @throws IOException when error to connect ChronosServer
   */
  private void reconnectChronosServer() throws IOException {
    LOG.info("Try to reconnect chronos server");
    
    if (transport != null) {
      transport.close();
    }
    connectChronosServer();
  }

  /**
   * Send RPC request to get timestamp from ChronosServer. Use lazy strategy to detect failure.
   * If request fails, reconnect ChronosServer. If request fails again, reconnect ZooKeeper.
   * 
   * @param range the number of timestamps
   * @return the first timestamp to use
   * @throws IOException when error to connect ChronosServer or ZooKeeper
   */
  public long getTimestamps(int range) throws IOException {
    long timestamp;
    try {
      timestamp = client.getTimestamps(range);
    } catch (TException e) {
      LOG.info("Can't get timestamp, try to connect the active chronos server");
      try {
        reconnectChronosServer();
        return client.getTimestamps(range);
      } catch (Exception e1) {
        LOG.info("Can't connect chronos server, try to connect ZooKeeper firstly");
        try {
          reconnectZooKeeper();
          reconnectChronosServer();
          return client.getTimestamps(range);
        } catch (Exception e2) {
          throw new IOException("Error to get timestamp after reconnecting ZooKeeper and chronos server", e2);
        }
      }
    }
    return timestamp;
  }
  
  /**
   * Provider the convenient method to get single timestamp.
   * 
   * @return the allocated timestamp
   * @throws IOException when error to get timestamp from ChronosServer
   */
  public long getTimestamp() throws IOException {
    return getTimestamps(1);
  }

  /**
   * Deal with connection event, just wait for a while when connected.
   * 
   * @param event ZooKeeper events
   */
  @Override
  public void process(WatchedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state="
          + event.getState() + ", " + "path=" + event.getPath());
    }

    if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
      try {
        waitToInitZooKeeper(2000); // init zookeeper in another thread, wait for a while
      } catch (Exception e) {
        LOG.error("Error to init ZooKeeper object after sleeping 2000 ms, reconnect ZooKeeper");
        try {
          reconnectZooKeeper();
        } catch (Exception e1) {
          LOG.error("Error to reconnect with ZooKeeper", e1);
        }
      }
    }
  }
  
   /**
    * Wait to init ZooKeeper object, only sleep when it's null.
    *
    * @param maxWaitMillis the max sleep time
    * @throws Exception if ZooKeeper object is still null
    */
   public void waitToInitZooKeeper(long maxWaitMillis) throws Exception {
     long finished = System.currentTimeMillis() + maxWaitMillis;
     while (System.currentTimeMillis() < finished) {
       if (this.zooKeeper != null) {
         return;
       }
       
       try {
         Thread.sleep(1);
       } catch (InterruptedException e) {
         throw new Exception(e);
       }
     }
     throw new Exception();
   }


  /**
   * Get the data from znode.
   * 
   * @param chronosClientWatcher the ZooKeeper watcher
   * @param znode the znode you want to access
   * @return the byte array of value in znode
   * @throws IOException when error to access ZooKeeper
   */
  public byte[] getData(ChronosClientWatcher chronosClientWatcher, String znode)
      throws IOException {
    byte[] data = null;
    for (int i = 0; i <= connectRetryTimes; i++) {
      try {
        data = chronosClientWatcher.getZooKeeper().getData(znode, null, null);
        break;
      } catch (Exception e) {
        LOG.info("Exceptioin to get data from ZooKeeper, retry " + i +" times");
        if (i == connectRetryTimes) {
          throw new IOException("Error when getting data from " + znode + " after retrying");
        }
      }
    }
    return data;
  }

  /**
   * Close the ZooKeeper object.
   */
  public void close() {
    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (InterruptedException e) {
        LOG.error("Interrupt to close zookeeper connection", e);
      }
    }
  }
  
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

}
