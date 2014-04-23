package com.xiaomi.infra.chronos.zookeeper;

import java.util.Properties;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Like HMaster in HBase, FailoverServer relies on ZooKeeper to implement fault-tolerant system.
 * Multiple servers can run concurrently but only one will be the active master, which is able to
 * run the business logic. Others have to block until the master znode changes.
 * 
 * @see FailoverWatcher
 */
public class FailoverServer {
  private static final Log LOG = LogFactory.getLog(FailoverServer.class);

  public static final String BASE_ZNODE = "baseZnode";
  public static final String ZK_QUORUM = "zkQuorum";
  public static final String SESSION_TIMEOUT = "sessionTimeout";
  public static final String CONNECT_RETRY_TIMES = "connectRetryTimes";
  public static final String ZK_SECURE = "zkSecure";
  public static final String ZK_ADMIN = "zkAdmin";
  public static final String JAAS_FILE = "jaasFile";
  public static final String KRB5_FILE = "krb5File";
  public static final String SERVER_HOST = "serverHost";
  public static final String SERVER_PORT = "serverPort";

  protected FailoverWatcher failoverWatcher;

  /**
   * Construct FailoverServer with FailoverWatcher.
   *
   * @param failoverWatcher the FailoverWatcher object
   */
  public FailoverServer(FailoverWatcher failoverWatcher) {
    this.failoverWatcher = failoverWatcher;
  }

  /**
   * Construct FailoverServer with properties.
   *
   * @param properties the properties of FailoverWatcher.
   * @throws IOException when error to construct FailoverWatcher
   */
  public FailoverServer(Properties properties) throws IOException {
    this(new FailoverWatcher(properties));
  }

  public FailoverWatcher getFailoverWatcher() {
    return failoverWatcher;
  }

  /**
   * The main logic of active/backup servers.
   */
  public void run() {
    LOG.info("The server runs and prepares for leader election");
    if (failoverWatcher.blockUntilActive()) {
      LOG.info("The server becomes active master and prepare to do business logic");
      doAsActiveServer();
    }
    failoverWatcher.close();
    LOG.info("The server exits after running business logic");
  }

  /**
   * The method you should override to do your business logic after becoming active master.
   */
  public void doAsActiveServer() {
    while (true) {
      LOG.info("No business logic, sleep for " + Integer.MAX_VALUE);
      try {
        Thread.sleep(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
        LOG.error("Interrupt when sleeping as active master", e);
      }
    }
  }

}
