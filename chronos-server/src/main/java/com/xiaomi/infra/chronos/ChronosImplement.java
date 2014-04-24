package com.xiaomi.infra.chronos;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.xiaomi.infra.chronos.exception.FatalChronosException;
import com.xiaomi.infra.chronos.exception.ChronosException;
import com.xiaomi.infra.chronos.generated.ChronosService;

/**
 * Implement the chronos.thrift and provide the interface to get timestamp. It will return the
 * precise auto-increasing timestamp which is based on the current wall-time. Update the value in
 * ZooKeeper to guarantee that the persistent timestamp is larger than any allocated value.
 */
public class ChronosImplement implements ChronosService.Iface {
  private static final Log LOG = LogFactory.getLog(ChronosImplement.class);

  private final ChronosServerWatcher chronosServerWatcher;
  private final long zkAdvanceTimestamp;
  private long maxAssignedTimestamp;
  private volatile boolean isAsyncSetPersistentTimestamp = false;

  /**
   * Construct ChronosImplement with properties and ChronosServerWatcher.
   * 
   * @param properties the properties of zkAdvanceTimestamp
   * @param chronosServerWatcher the ZooKeeper client to set persistent timestamp
   * @throws FatalChronosException when set a smaller timestamp in ZooKeeper
   * @throws ChronosException when error to set value in ZooKeeper
   */
  public ChronosImplement(Properties properties, ChronosServerWatcher chronosServerWatcher)
      throws FatalChronosException, ChronosException {
    this.chronosServerWatcher = chronosServerWatcher;
    this.zkAdvanceTimestamp = Long.parseLong(properties.getProperty(
      ChronosServer.ZK_ADVANCE_TIMESTAMP, "1000"));
  }

  /**
   * Assign required number of timestamps, client can use [timestamp, timestamp + range).
   * 
   * @param range, the number of timestamps to assign
   * @return timestamp, the first available timestamp to client
   */
  public long getTimestamps(int range) throws TException {

    // can get 2^18(262144) times for each millisecond for about 1115 years
    long currentTime = System.currentTimeMillis() << 18;
    synchronized (this) {
      // maxAssignedTimestamp is assigned last time, can't return currentTime when it's less or equal
      if (currentTime > maxAssignedTimestamp) {
        maxAssignedTimestamp = currentTime + range - 1;
      } else {
        maxAssignedTimestamp += range;
      }
      // now [maxAssignedTimestamp - range + 1, maxAssignedTimestamp] will be returned

      // for correctness, compare with persistent timestamp and set it if necessary
      if (maxAssignedTimestamp >= chronosServerWatcher.getCachedPersistentTimestamp()) {

        // wait for the result of asyn set
        sleepUntilAsyncSet();

        // sync set persistent timestamp if necessary
        if (maxAssignedTimestamp >= chronosServerWatcher.getCachedPersistentTimestamp()) {
          long newPersistentTimestamp = maxAssignedTimestamp + zkAdvanceTimestamp;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Try to sync set persistent timestamp " + newPersistentTimestamp);
          }
          try {
            chronosServerWatcher.setPersistentTimestamp(newPersistentTimestamp);
          } catch (ChronosException e) {
            LOG.fatal("Error to set persistent timestamp, exit immediately");
            System.exit(0);
          }
        }

      }

      // for performance, async set persistent timestamp before reaching persistent timestamp
      if (!isAsyncSetPersistentTimestamp
          && maxAssignedTimestamp >= chronosServerWatcher.getCachedPersistentTimestamp()
              - zkAdvanceTimestamp * 0.5) {
        long newPersistentTimestamp = chronosServerWatcher.getCachedPersistentTimestamp()
            + zkAdvanceTimestamp;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Try to async set persistent timestamp " + newPersistentTimestamp);
        }
        isAsyncSetPersistentTimestamp = true;
        asyncSetPersistentTimestamp(newPersistentTimestamp);
      }

      // return the first available timestamp
      return maxAssignedTimestamp - range + 1;
    }
  }

  /**
   * Provide a convenient interface to get a single timestamp.
   *
   * @return the allocated timestamp
   * @throws TException when error to response thrift request
   */
  public long getTimestamp() throws TException {
    return getTimestamps(1);
  }

  /**
   * Sleep until asynchronously set persistent timestamp successfully.
   */
  private void sleepUntilAsyncSet() {
    LOG.info("Sleep a while until asynchronously set persistent timestamp");
    while (isAsyncSetPersistentTimestamp) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        LOG.fatal("Interrupt when sleep to set persistent timestamp, exit immediately");
        System.exit(0);
      }
    }
  }

  /**
   * Get the persistent timestamp in ZooKeeper and initialize the new one in ZooKeeper.
   *
   * @throws ChronosException when error to set value in ZooKeeper
   * @throws FatalChronosException when set a smaller timestamp in ZooKeeper
   */
  public void initTimestamp() throws ChronosException, FatalChronosException {
    maxAssignedTimestamp = chronosServerWatcher.getPersistentTimestamp();
    long newPersistentTimestamp = maxAssignedTimestamp + zkAdvanceTimestamp;
    chronosServerWatcher.setPersistentTimestamp(newPersistentTimestamp);
    LOG.info("Get persistent timestamp " + maxAssignedTimestamp + " and set "
        + newPersistentTimestamp + " in ZooKeeper");
  }

  /**
   * Create a new thread to asynchronously set persistent timestamp in ZooKeeper.
   *
   * @param newPersistentTimestamp the new timestamp to set
   */
  public synchronized void asyncSetPersistentTimestamp(final long newPersistentTimestamp) {
    new Thread() {
      @Override
      public void run() {
        try {
          chronosServerWatcher.setPersistentTimestamp(newPersistentTimestamp);
          isAsyncSetPersistentTimestamp = false;
        } catch (Exception e) {
          LOG.fatal("Error to set persistent timestamp, exit immediately");
          System.exit(0);
        }
      }
    }.start();
  }

  public ChronosServerWatcher getChronosServerWatcher() {
    return chronosServerWatcher;
  }

}
