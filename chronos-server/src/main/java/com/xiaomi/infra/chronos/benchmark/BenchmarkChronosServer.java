package com.xiaomi.infra.chronos.benchmark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xiaomi.infra.chronos.client.ChronosClient;

/**
 * The tool to benchmark ChronosServer and print the metrics.
 */
public class BenchmarkChronosServer {
  private static final Log LOG = LogFactory.getLog(BenchmarkChronosServer.class);

  private ChronosClient chronosClient;

  private long startTime;
  private AtomicInteger totalCountInteger = new AtomicInteger();
  private AtomicInteger totalLatencyInteger = new AtomicInteger();
  private long previousTimestamp = 0;
  private long currentTimestamp;
  private long failoverStartTime = 0;
  private String failoverStartTimeString;
  private boolean isFailover = false;

  /**
   * Construct BenchmarkChronosServer with zkQuorum and clusterName.
   * 
   * @param zkQuorum the ZooKeeper quorum string for ChronosClientWatcher
   * @throws IOException when error to construct ChronosClientWatcher
   */
  public BenchmarkChronosServer(String zkQuorum, String clusterName) throws IOException {
    chronosClient = new ChronosClient(zkQuorum, clusterName);
  }

  /**
   * Benchmark ChronosServer and print metrics in another thread.
   */
  public void startBenchmark() {
    startTime = System.currentTimeMillis();
    LOG.info("Start to benchmark chronos server");

    Thread t = new Thread() { // new thread to output the metrics
      @Override
      public void run() {
        final long collectPeriod = 10000;
        LOG.info("Start another thread to export benchmark metrics every " + collectPeriod / 1000.0
            + " second");

        int totalCount;
        int totalLatency;
        long exportTime;
        int lastTotalCount = 0;
        int lastTotalLatency = 0;
        long lastExportTime = startTime;

        while (true) {
          try {
            Thread.sleep(collectPeriod);
          } catch (InterruptedException e) {
            LOG.error("Interrupt when sleep to get benchmark metrics, exit immediately");
            System.exit(0);
          }

          exportTime = System.currentTimeMillis();
          totalCount = totalCountInteger.get();
          totalLatency = totalLatencyInteger.get();

          double totalCostTime = (exportTime - startTime) / 1000.0;
          double costTime = (exportTime - lastExportTime) / 1000.0;
          double qps = (totalCount - lastTotalCount) / costTime;
          double latency = (totalLatency - lastTotalLatency) * 1.0 / (totalCount - lastTotalCount);
          System.out.println("Total " + totalCostTime + ", in " + costTime + " seconds, qps: "
              + qps + ", latency: " + latency + "ms");

          lastTotalCount = totalCount;
          lastTotalLatency = totalLatency;
          lastExportTime = exportTime;
        }
      }
    };

    t.setDaemon(true);
    t.start();

    while (true) {
      try {
        long start = System.currentTimeMillis();
        currentTimestamp = chronosClient.getTimestamp();
        totalCountInteger.incrementAndGet();
        totalLatencyInteger.addAndGet((int) (System.currentTimeMillis() - start));

        if (currentTimestamp <= previousTimestamp) { // check correctness
          LOG.error("Fatal error to get a lower timestamp " + currentTimestamp + "(previous is "
              + previousTimestamp + "), exit immediately");
          System.exit(0);
        }
        previousTimestamp = currentTimestamp;

        if (isFailover == true) { // calculate failover time
          double failoverTime = (System.currentTimeMillis() - failoverStartTime) / 1000.0;
          System.out.println("After " + failoverStartTimeString + ", the total failover time is "
              + failoverTime + " seconds");
        }
        isFailover = false;
      } catch (IOException e) {
        LOG.error("Exception to get timestamp");

        if (isFailover == false) {
          failoverStartTime = System.currentTimeMillis();
          failoverStartTimeString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(
              failoverStartTime));
          LOG.info("Failover occurs at " + failoverStartTimeString);
        }
        isFailover = true;
      }
    }

  }

  /**
   * The command-line tool to benchmark ChronosServer and print metrics.
   * Usage: mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.benchmark.BenchmarkChronosServer" -Dexec.args="$zkQuorum $clientNumber"
   * 
   * @param argv the first one is zkQuorum and the optional second one is clientNumber
   */
  public static void main(String[] argv) {
    if (argv.length < 2 || argv.length > 3) {
      System.err.println("Wrong parameters, exit immediately");
      return;
    }

    final String zkQuorum = argv[0];
    final String clusterName = argv[1];
    int clientNumber = 1;
    if (argv.length == 3) {
      clientNumber = Integer.parseInt(argv[2]);
    }

    for (int i = 0; i < clientNumber; ++i) {
      new Thread() {
        @Override
        public void run() {
          try {
            BenchmarkChronosServer benchmark = new BenchmarkChronosServer(zkQuorum, clusterName);
            benchmark.startBenchmark();
          } catch (IOException e) {
            System.err.println("Error to connect with ZooKeeper or ChronosServer to benchmark");
          }
        }
      }.start();
    }

  }

}
