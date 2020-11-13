package com.xiaomi.infra.chronos;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.xiaomi.infra.chronos.exception.FatalChronosException;
import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.zookeeper.FailoverWatcher;
import com.xiaomi.infra.chronos.zookeeper.ZooKeeperUtil;

/**
 * The ZooKeeper watcher for ChronosServer provides the interface to get the persistent timestamp
 * which is the max value allocated currently.
 */
public class ChronosServerWatcher extends FailoverWatcher {
  private static final Log LOG = LogFactory.getLog(ChronosServerWatcher.class);

  private final String persistentTimestampZnode;
  private boolean beenActiveMaster = false;
  private long persistentTimestamp;

  /**
   * Construct ChronosServerWatcher with properties.
   *
   * @param properties the properties of ChronosServerWatcher
   * @param canInitZnode whether it can create znode or not
   * @throws IOException when it's error to construct ZooKeeper watcher
   */
  public ChronosServerWatcher(Properties properties, boolean canInitZnode) throws IOException {
    super(properties, canInitZnode);
    
    persistentTimestampZnode = baseZnode + "/persistent-timestamp";
  }

  /**
   * Construct ChronosServerWatcher with properties.
   *
   * @param properties the properties of ChronosServerWatcher
   * @throws IOException when it's error to construct ZooKeeper watcher
   */
  public ChronosServerWatcher(Properties properties) throws IOException {
    this(properties, true);
  }

  /**
   * Create the base znode of chronos.
   */
  @Override
  protected void initZnode() { 
    try {
      ZooKeeperUtil.createAndFailSilent(this, "/chronos");
      super.initZnode();
      ZooKeeperUtil.createAndFailSilent(this, baseZnode + "/persistent-timestamp");
    } catch (Exception e) {
      LOG.fatal("Error to create znode of chronos, exit immediately");
      System.exit(0);
    }
  }

  /**
   * Get persistent timestamp in ZooKeeper with retries.
   *
   * @return the persistent timestamp in ZooKeeper
   * @throws ChronosException error to get data from ZooKeeper after retrying
   */
  public long getPersistentTimestamp() throws ChronosException {
    byte[] persistentTimesampBytes = null;
    for (int i = 0; i <= connectRetryTimes; ++i) {
        persistentTimesampBytes = ZooKeeperUtil.getDataAndWatch(this, persistentTimestampZnode);

        if (Objects.nonNull(persistentTimesampBytes)) {
          break;
        }

        if (i == connectRetryTimes) {
          throw new ChronosException(
              "Can't get persistent timestamp from ZooKeeper after retrying");
        }
        LOG.info("Exception to get persistent timestamp from ZooKeeper, retry " + (i + 1) + " times");
    }

    if (Arrays.equals(persistentTimesampBytes, new byte[0])) {
      persistentTimestamp = 0; // for the very first time
    } else {
      persistentTimestamp = ZooKeeperUtil.bytesToLong(persistentTimesampBytes);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Return persistent timestamp " + persistentTimestamp + " to timestamp server");
    }
    return persistentTimestamp;
  }

  /**
   * Get the cached timestamp in ChronosServerWatcher.
   *
   * @return the local persistentTimestamp
   */
  public long getCachedPersistentTimestamp() {
    return persistentTimestamp;
  }

  /**
   * Set persistent timestamp in ZooKeeper.
   *
   * @param newTimestamp the new value to set
   * @throws FatalChronosException if try to set a small value
   * @throws ChronosException if error to set value in ZooKeeper
   */
  public void setPersistentTimestamp(long newTimestamp) throws FatalChronosException,
      ChronosException {
    if (newTimestamp <= persistentTimestamp) {
      throw new FatalChronosException("Fatal error to set a smaller timestamp");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting persistent timestamp " + newTimestamp + " in ZooKeeper");
    }

    for (int i = 0; i <= connectRetryTimes; ++i) {
      try {
        ZooKeeperUtil.setData(this, persistentTimestampZnode, ZooKeeperUtil.longToBytes(newTimestamp));
        persistentTimestamp = newTimestamp;
        break;
      } catch (KeeperException e) {
        if (i == connectRetryTimes) {
          throw new ChronosException(
              "Error to set persistent timestamp in ZooKeeper after retrying", e);
        }
        LOG.info("Exception to set persistent timestamp in ZooKeeper, retry " + (i + 1) + " times");
      }
    }
  }

  /**
   * Deal with the connection event.
   *
   * @param event the ZooKeeper event.
   */
  @Override
  protected void processConnection(WatchedEvent event) {
    super.processConnection(event);
    if (event.getState() == Event.KeeperState.Disconnected) {
      if (beenActiveMaster) {
        LOG.fatal(hostPort.getHostPort()
            + " disconnected from ZooKeeper, stop serving and exit immediately");
        System.exit(0);
      } else {
        LOG.warn(hostPort.getHostPort()
            + " disconnected from ZooKeeper, wait to sync and try to become active master");
      }
    }
  }

  public String getPersistentTimestampZnode(){
    return persistentTimestampZnode;
  }
  
  public void setBeenActiveMaster(boolean beenActiveMaster) {
    this.beenActiveMaster = beenActiveMaster;
  }
  
}