package com.xiaomi.infra.chronos.client;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * The client of ChronosServer provides an interface to get precise auto-increasing timestamp. It
 * will throw IOException for any error during connecting with server or getting timestamp.
 * 
 * @see ChronosClientWatcher
 */
public class ChronosClient {
  
  public static final String ZK_QUORUM = "zkQuorum";
  public static final String CLUSTER_NAME = "clusterName";
  public static final String SESSION_TIMEOUT = "sessionTimeout";
  public static final String CONNECT_RETRY_TIMES = "connectRetryTimes";
  public static final String SOCKET_TIMEOUT = "socketTimeout";

  private ChronosClientWatcher chronosClientWatcher;

  /**
   * Construct ChronosClient with ChronosClientWatcher.
   * 
   * @param chronosClientWatcher the ChronosClientWatcher object
   */
  public ChronosClient(ChronosClientWatcher chronosClientWatcher) {
    this.chronosClientWatcher = chronosClientWatcher;
  }
  
  /**
   * Construct ChronosClient with properties.
   * 
   * @param properties the properties of ChronosClient
   * @throws IOException when error to construct ChronosClientWatcher
   */
  public ChronosClient(Properties properties) throws IOException {
    this.chronosClientWatcher = new ChronosClientWatcher(properties);
  }
  
  /**
   * Construct ChronosClient just with ZkQuorum and clusterName, use default properties.
   * 
   * @param zkQuorum the ZooKeeper quorum string
   * @throws IOException when error to construct ChronosClientWatcher
   */
  public ChronosClient(String zkQuorum, String clusterName) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ZK_QUORUM, zkQuorum);
    properties.setProperty(CLUSTER_NAME, clusterName);
    properties.setProperty(SESSION_TIMEOUT, String.valueOf(30000));
    properties.setProperty(CONNECT_RETRY_TIMES, String.valueOf(10));
    properties.setProperty(SOCKET_TIMEOUT, String.valueOf(3000));
    this.chronosClientWatcher = new ChronosClientWatcher(properties);
  }

  /**
   * Get timestamps from ChronosClientWatcher.
   * 
   * @param range the number of timestamps
   * @return the first timestamp to use
   * @throws IOException when error to connect ChronosServer or ZooKeeper
   */
  public long getTimestamps(int range) throws IOException {
    return chronosClientWatcher.getTimestamps(range);
  }
  
  /**
   * Get timestamp from ChronosClientWatcher.
   * 
   * @return the timestamp to use
   * @throws IOException  when error to connect ChronosServer or ZooKeeper
   */
  public long getTimestamp() throws IOException {
    return chronosClientWatcher.getTimestamp();
  }
  
  public void close() {
    if (this.chronosClientWatcher != null) {
      this.chronosClientWatcher.close();
    }
  }

  /**
   * The command-line tool to use ChronosClient to get a timestamp.
   * Usage: mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.client.ChronosClient" -Dexec.args="$zkQuorum $clusterName"
   * 
   * @param argv first argument is ZooKeeper quorum string
   */
  public static void main(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.err.println("Wrong parameters, exit immediately");
      return;
    }
    
    ChronosClient chronosClient = null;
    try {
      chronosClient = new ChronosClient(argv[0], argv[1]);
      System.out.println("Get timestamp " + chronosClient.getTimestamp());
    } catch (IOException e) {
      System.err.println("Error to connect with ZooKeeper or ChronosServer, check the configuration");
      throw e;
    } finally {
      if (Objects.nonNull(chronosClient)) {
        chronosClient.close();
      }
    }
  }

  public ChronosClientWatcher getChronosClientWatcher(){
    return chronosClientWatcher;
  }
  
}
