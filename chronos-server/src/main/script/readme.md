# Chronos scripts

## Setup
* `bundle install` to install dependent libraries.

## Usage

### list_servers.rb
* List all the running chronos servers and allocated timestamp.
* Give ZooKeeper quorum string and run `ruby list_servers.rb $zkQuorum`.
* Example: `ruby list_servers.rb 127.0.0.1:2181`

> Connected ZooKeeper 127.0.0.1:2181
Master is 10.0.200.300_7911
Backup master is 10.0.200.301_7911
Backup master is 10.0.200.302_7911
persistent-timestamp is 365565981241901056 (2014-03-11 15:39:09 +0800)

### translate_timestamp.rb
* Translate the chronos timestamp into readable world time.
* Give timestamp and run `ruby translate_timestamp.rb $timestamp`.
* Example: `ruby translate_timestamp.rb 365565981241901056`

> 2014-03-11 15:39:09 +0800

### process_benchmark_log.rb
* After benchmarking chronos servers, process the log to print qps and latency.
* Give the benchmark log file and run `ruby process_benchmark_log.rb $file`.
* Example: `ruby process_benchmark_log.rb chronos.log`

> Average failover time: 4.174609442060084
Average qps: 6869.169584139136
Average latency: 0.1446603355816408 
