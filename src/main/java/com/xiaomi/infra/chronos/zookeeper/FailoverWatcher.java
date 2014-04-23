package com.xiaomi.infra.chronos.zookeeper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.xiaomi.infra.chronos.ChronosServerWatcher;

/**
 * Like ActiveMasterManager in HBase, FailoverWatcher implements master/backup servers switching
 * with ZooKeeper. It will store the information of servers in znode and deal with the detail of
 * blocking and notification for leader election.
 */
public class FailoverWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(FailoverWatcher.class);

  protected final Properties properties;
  protected final String baseZnode;
  protected final String masterZnode;
  protected final String backupServersZnode;
  protected final String zkQuorum;
  protected final int sessionTimeout;
  protected final int connectRetryTimes;
  protected final boolean isZkSecure;
  protected final String zkAdmin;
  protected final String jaasFile;
  protected final String krb5File;
  protected final HostPort hostPort;
  
  protected ZooKeeper zooKeeper;
  private final AtomicBoolean hasActiveServer = new AtomicBoolean(false);

  /**
   * Initialize FailoverWatcher with properties.
   * 
   * @param properties the basic settings for FailoverWathcer
   * @param canInitZnode should create the base znode or not
   * @throws IOException throw when can't connect with ZooKeeper
   */
  public FailoverWatcher(Properties properties, boolean canInitZnode) throws IOException {
    this.properties = properties;

    baseZnode = properties.getProperty(FailoverServer.BASE_ZNODE, "/failover-server");
    masterZnode = baseZnode + "/master";
    backupServersZnode = baseZnode + "/backup-servers";
    zkQuorum = properties.getProperty(FailoverServer.ZK_QUORUM, "127.0.0.1:2181");
    sessionTimeout = Integer.parseInt(properties
        .getProperty(FailoverServer.SESSION_TIMEOUT, "5000"));
    connectRetryTimes = Integer.parseInt(properties.getProperty(FailoverServer.CONNECT_RETRY_TIMES,
      "10"));
    isZkSecure = Boolean.parseBoolean(properties.getProperty(FailoverServer.ZK_SECURE, "false"));
    zkAdmin = properties.getProperty(FailoverServer.ZK_ADMIN, "h_chronos_admin");
    jaasFile = properties.getProperty(FailoverServer.JAAS_FILE, "../conf/jaas.conf");
    krb5File = properties.getProperty(FailoverServer.KRB5_FILE, "/etc/krb5.conf");
    String serverHost = properties.getProperty(FailoverServer.SERVER_HOST, "127.0.0.1");
    int serverPort = Integer.parseInt(properties.getProperty(FailoverServer.SERVER_PORT, "10086"));
    hostPort = new HostPort(serverHost, serverPort);

    if (isZkSecure) {
      LOG.info("Connect with secure ZooKeeper cluster, use " + jaasFile + " and " + krb5File);
      System.setProperty("java.security.auth.login.config", jaasFile);
      System.setProperty("java.security.krb5.conf", krb5File);
    }

    connectZooKeeper();

    if (canInitZnode) {
      initZnode();
    }
  }

  /**
   * Construct FailoverWatcher with properties, create znode by default.
   *
   * @param properties the properties of FailoverWatcher
   * @throws IOException when error to construct FailoverWatcher
   */
  public FailoverWatcher(Properties properties) throws IOException {
    this(properties, true);
  }

  /**
   * Connect with ZooKeeper with retries.
   *
   * @throws IOException when error to construct ZooKeeper object after retrying.
   */
  protected void connectZooKeeper() throws IOException {
    LOG.info("Connecting ZooKeeper " + zkQuorum);

    for (int i = 0; i <= connectRetryTimes; i++) {
      try {
        zooKeeper = new ZooKeeper(zkQuorum, sessionTimeout, this);
        break;
      } catch (IOException e) {
        if (i == connectRetryTimes) {
          throw new IOException("Can't connect ZooKeeper after retrying", e);
        }
        LOG.error("Exception to connect ZooKeeper, retry " + (i + 1) + " times");
      }
    }
  }

  /**
   * Initialize the base znodes of chronos.
   */
  protected void initZnode() {
    try {
      ZooKeeperUtil.createAndFailSilent(this, baseZnode);
      ZooKeeperUtil.createAndFailSilent(this, backupServersZnode);
    } catch (Exception e) {
      LOG.fatal("Error to create znode " + baseZnode + " and " + backupServersZnode
          + ", exit immediately", e);
      System.exit(0);
    }
  }

  /**
   * Override this mothod to deal with events for leader election.
   *
   * @param event the ZooKeeper event.
   */
  @Override
  public void process(WatchedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state="
          + event.getState() + ", " + "path=" + event.getPath());
    }

    switch (event.getType()) {
    case None: {
      processConnection(event);
      break;
    }
    case NodeCreated: {
      processNodeCreated(event.getPath());
      break;
    }
    case NodeDeleted: {
      processNodeDeleted(event.getPath());
      break;
    }
    case NodeDataChanged: {
      processDataChanged(event.getPath());
      break;
    }
    case NodeChildrenChanged: {
      processNodeChildrenChanged(event.getPath());
      break;
    }
    default:
      break;
    }
  }

  /**
   * Deal with connection event, exit current process if auth fails or session expires.
   *
   * @param event the ZooKeeper event.
   */
  protected void processConnection(WatchedEvent event) {
    switch (event.getState()) {
    case SyncConnected:
      LOG.info(hostPort.getHostPort() + " sync connect from ZooKeeper");
      try {
        waitToInitZooKeeper(2000); // init zookeeper in another thread, wait for a while
      } catch (Exception e) {
        LOG.fatal("Error to init ZooKeeper object after sleeping 2000 ms, exit immediately");
        System.exit(0);
      } 
      break;
    case Disconnected: // be triggered when kill the server or the leader of zk cluster 
      LOG.warn(hostPort.getHostPort() + " received disconnected from ZooKeeper");
      break;
    case AuthFailed:
      LOG.fatal(hostPort.getHostPort() + " auth fail, exit immediately");
      System.exit(0);
    case Expired:
      LOG.fatal(hostPort.getHostPort() + " received expired from ZooKeeper, exit immediately");
      System.exit(0);
      break;
    default:
      break;
    }
  }

  /**
   * Deal with create node event, just call the leader election.
   *
   * @param path which znode is created
   */
  protected void processNodeCreated(String path) {
    if (path.equals(masterZnode)) {
      LOG.info(masterZnode + " created and try to become active master");
      handleMasterNodeChange();
    }
  }

  /**
   * Deal with delete node event, just call the leader election.
   *
   * @param path which znode is deleted
   */
  protected void processNodeDeleted(String path) {
    if (path.equals(masterZnode)) {
      LOG.info(masterZnode + " deleted and try to become active master");
      handleMasterNodeChange();
    }
  }

  /**
   * Do nothing when data changes, should be overrided.
   *
   * @param path which znode's data is changed
   */
  protected void processDataChanged(String path) {

  }

  /**
   * Do nothing when children znode changes, should be overrided.
   *
   * @param path which znode's children is changed.
   */
  protected void processNodeChildrenChanged(String path) {

  }

  /**
   * Implement the logic of leader election.
   */
  private void handleMasterNodeChange() {
    try {
      synchronized (hasActiveServer) {
        if (ZooKeeperUtil.watchAndCheckExists(this, masterZnode)) {
          // A master node exists, there is an active master
          if (LOG.isDebugEnabled()) {
            LOG.debug("A master is now available");
          }
          hasActiveServer.set(true);
        } else {
          // Node is no longer there, cluster does not have an active master
          if (LOG.isDebugEnabled()) {
            LOG.debug("No master available. Notifying waiting threads");
          }
          hasActiveServer.set(false);
          // Notify any thread waiting to become the active master
          hasActiveServer.notifyAll();
        }
      }
    } catch (KeeperException ke) {
      LOG.error("Received an unexpected KeeperException, aborting", ke);
    }
  }

  /**
   * Implement the logic of server to wait to become active master.
   *
   * @return false if error to wait to become active master.
   */
  public boolean blockUntilActive() {
    while (true) {
      try {
        if (ZooKeeperUtil.createEphemeralNodeAndWatch(this, masterZnode, hostPort.getHostPort()
            .getBytes())) {
          // If we were a backup master before, delete our ZNode from the backup
          // master directory since we are the active now
          LOG.info("Deleting ZNode for " + backupServersZnode + "/" + hostPort.getHostPort()
              + " from backup master directory");
          ZooKeeperUtil.deleteNodeFailSilent(this,
            backupServersZnode + "/" + hostPort.getHostPort());

          // We are the master, return
          hasActiveServer.set(true);
          LOG.info("Become active master in " + hostPort.getHostPort());
          return true;
        }

        hasActiveServer.set(true);

        /*
         * Add a ZNode for ourselves in the backup master directory since we are not the active
         * master. If we become the active master later, ActiveMasterManager will delete this node
         * explicitly. If we crash before then, ZooKeeper will delete this node for us since it is
         * ephemeral.
         */
        LOG.info("Adding ZNode for " + backupServersZnode + "/" + hostPort.getHostPort()
            + " in backup master directory");
        ZooKeeperUtil.createEphemeralNodeAndWatch(this,
          backupServersZnode + "/" + hostPort.getHostPort(), hostPort.getHostPort().getBytes());

        // we start the server with the same ip_port stored in master znode, that means we want to
        // restart the server?
        String msg;
        byte[] bytes = ZooKeeperUtil.getDataAndWatch(this, masterZnode);
        if (bytes == null) {
          msg = ("A master was detected, but went down before its address "
              + "could be read.  Attempting to become the next active master");
        } else {
          if (hostPort.getHostPort().equals(new String(bytes))) {
            msg = ("Current master has this master's address, " + hostPort.getHostPort() + "; master was restarted? Deleting node.");
            // Hurry along the expiration of the znode.
            ZooKeeperUtil.deleteNode(this, masterZnode);
          } else {
            msg = "Another master " + new String(bytes) + " is the active master, "
                + hostPort.getHostPort() + "; waiting to become the next active master";
          }
        }
        LOG.info(msg);
      } catch (KeeperException ke) {
        LOG.error("Received an unexpected KeeperException when block to become active, aborting",
          ke);
        return false;
      }

      synchronized (hasActiveServer) {
        while (hasActiveServer.get()) {
          try {
            hasActiveServer.wait();
          } catch (InterruptedException e) {
            // We expect to be interrupted when a master dies, will fall out if so
            if (LOG.isDebugEnabled()) {
              LOG.debug("Interrupted while waiting to be master");
            }
            return false;
          }
        }
      }
    }
  }

  /**
   * Close the ZooKeeper object.
   */
  public void close() {
    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (InterruptedException e) {
        LOG.error("Interrupt when closing zookeeper connection", e);
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
  
  public String getBaseZnode() {
    return baseZnode;
  }
  
  public String getMasterZnode() {
    return masterZnode;
  }

  public String getBackupServersZnode() {
    return backupServersZnode;
  }
  
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  public boolean hasActiveServer() {
    return hasActiveServer.get();
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  public Properties getProperties() {
    return properties;
  }
  
  public boolean isZkSecure() {
    return isZkSecure;
  }
  
  public String getZkAdmin() {
    return zkAdmin;
  }

}