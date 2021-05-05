# Chronos

## 简介

Chronos，在古希腊语意为[时间](http://en.wikipedia.org/wiki/Chronos)，是实现**高可用**、**高性能**、**提供全局唯一而且严格单调递增timestamp**的服务。

Chronos采用主备架构，主服务器挂了以后备服务器迅速感知并接替服务，从而实现系统的高可用。服务端使用[Thrift](http://thrift.apache.org/)框架，经测试每秒可处理约60万次RPC请求，客户端单线程每秒可请求6万次(本地服务器)，保证高性能与低延时。全局只有唯一的ChronosServer提供服务，分配的timestamp保证严格单调递增，并且将已分配的值持久化到ZooKeeper上，即使发生failover也能保证服务的正确性。

## 原理

![chronos architecture](https://raw.github.com/XiaoMi/chronos/master/chronos_architecture.png)

Chronos依赖[ZooKeeper](http://zookeeper.apache.org/)实现与[HBase](https://hbase.apache.org/)类似的Leader Election机制，ChronosServer启动时将自己的信息写到ZooKeeper的Master临时节点上，如果主服务器已经存在，那么就记录到BackupServers节点上。一旦Master临时节点消失(主服务器发生failover)，所有备服务器收到ZooKeeper通知后参与新一轮的选主，保证最终只有一个新的主服务器接替服务。

ChronosServer运行时会启动一个Thrift服务器，提供getTimestamp()和getTimestamps(int)接口，并且保证每次返回的timestamp都是严格单调递增的。返回的timestamp与现实时间有基本对应关系，为当前Unix time乘以2的18次方(足够使用1115年)，由于我们优化了性能，所以如果存在failover就**不能保证这种对应关系的可靠性**。

ChronosClient启动时，通过访问ZooKeeper获得当前的主ChronosServer地址，连接该服务器后就可以发送Thrift RPC请求了。一旦主服务器发生failover，客户端请求失败，它会自动到ZooKeeper获得新的主ChronosServer地址重新建立连接。

## 使用

### Chronos服务端

1. 进入chronos-server目录，通过`mvn clean package -DskipTests`编译源码。
2. 进入target里面的conf目录，编辑chronos.conf，填写依赖的ZooKeeper配置。
3. 进入target里面的bin目录，执行`sh ./chronos.sh`既可运行ChronosServer。

### Chronos客户端

1. 进入chronos-client目录，通过`mvn clean package -DskipTests`编译源码。
2. 客户端在pom.xml添加chronos-client的依赖(**请使用对应的Thrift版本**)。

     ```
     <dependency>
       <groupId>com.xiaomi.infra</groupId>
       <artifactId>chronos-client</artifactId>
       <version>1.2.0-thrift0.5.0</version>
     </dependency>
     ```
     
3. 创建ChronosClient对象，如`new ChronosClient("127.0.0.1:2181", "default-cluster")`。
4. 发送RPC请求，如`chronosClient.getTimestamp()`或`chronosClient.getTimestamps(10)`。

### 快速体验

1. 参考[ZooKeeper文档](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html)，编译ZooKeeper并运行在127.0.0.1:2181上。
2. 获得chronos源代码，执行`mvn clean package -DskipTests`编译(需要安装Thrift)。
3. 进入chronos-server的bin目录，执行`sh ./chronos.sh`运行ChronosServer。
4. 进入chronos-client目录，执行`mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.client.ChronosClient" -Dexec.args="127.0.0.1:2181 default-cluster"`。

## 场景

* 提供全局严格单调递增的timestamp，用于实现[Percolator](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36726.pdf)等全局性事务。
* 提供全局唯一的值，相比[snowflake](https://github.com/twitter/snowflake)不依赖NTP服务，并且提供failover机制。

## 工具

* 提供list_servers.rb脚本，可监控当前的所有运行的ChronosServer状态。
* 提供translate_timestamp.rb脚本，可将timestamp转化为可读的世界时间。
* 提供process_benchmark_log.rb脚本，可处理Benchmark程序产生的日志。

## 测试

* 性能测试

| 客户端线程数 | 平均QPS | 平均Latency(毫秒) | 平均Failover时间(秒) | 服务端总QPS |
|-------------|---------|------------------|---------------------|------------|
| 1      | 10792.757    | 0.093            | 3.056               | 32378.271  |
| 10     | 7919.679     | 0.127            | 3.053               | 237590.370 |
| 20     | 6676.801     | 0.164            | 3.952               | 400788.060 |
| 50     | 3954.026     | 0.255            | 4.044               | 593103.900 |
| 100    | 1791.251     | 0.605            | 5.470               | 537375.300 |
| 50(最优) | 3993.749   | 0.251            | 0.000               | 599062.350 |

* Failover测试

持续杀掉ChronosServer和ZooKeeper进程没有发现正确性问题，failover时间符合预期。

注意，**Failover时间可通过ZooKeeper的tickTime和Chronos的sessionTimeout来设置**，**线上部署时应配合[Supervisor](http://supervisord.org/)或者[God](http://godrb.com/)来监控和拉起服务**。

---

# Chronos

## Introduction

Chronos, known as ["time" in Ancient Greek](http://en.wikipedia.org/wiki/Chronos), is the **high-availability**, **high-performance** service to provide **globally strictly monotone increasing timestamp**. 

Chronos uses standby architecture so that when the master server is down, any backup server could notice and take over the service. We use [Thrift](http://thrift.apache.org/) as RPC server, which could handle 600,000 QPS for server and 60,000 for client(with local server). All the timestamps are allocated by one master server so we can make sure they're unique and monotone increasing. It relies on ZooKeeper to store persistent data. Even if the master sever fails, backup servers could keep on allocating increasing timestamp as well.

## How It Works

![chronos architecture](https://raw.github.com/XiaoMi/chronos/master/chronos_architecture.png)

Chronos implements the leader election with [ZooKeeper](http://zookeeper.apache.org/), which is like [HBase](https://hbase.apache.org/). ChronosServer tries to register itself in the ephemeral master znode when it starts up. But if the master server already existes, it will register in the backup servers znode. Once the master znode disappears(the master server fails), ZooKeeper will notify any backup server for leader election and finally choose one new master. 

ChronosServer starts a Thrift server when it's running. We have implemented two interfaces, `getTimestamp()` and `getTimestamps(int)`, and make sure that all the timestamps are strictly monotone increasing. Furthermore, this timestamp is based on the world time. But we don't guarantee that they're always the same if the master fails frequently.

The ChronosClient will get the address of master ChronosServer in ZooKeeper. Then  it's able to send RPC requests to get timestamp. Once the master server fails, the requests fail as well, then ChronosClient will find out the new master server through ZooKeeper and recover.

## Usage

### Chronos-server

1. Enter the directory of chronos-server, compile through `mvn clean package -DskipTests`.
2. Enter the directory of conf, edit chronos.conf and fill in the ZooKeeper you're using.
3. Enter the directory of bin, execute `sh ./chronos.sh` to monitor the running status.

### Chronos-client

1. Enter the directory of chronos-client, compile through `mvn clean package -DskipTests`.
2. Add the dependency in pom.xml(**with the same version of Thrift**).

     ```
     <dependency>
       <groupId>com.xiaomi.infra</groupId>
       <artifactId>chronos-client</artifactId>
       <version>1.2.0-thrift0.5.0</version>
     </dependency>
     ```

3. Construct the chronos client object, like `new ChronosClient("127.0.0.1:2181", "default-cluster")`.
4. Send RPC request through client, like `chronosClient.getTimestamp()` or `chronosClient.getTimestamps(10)`.

### Quick Use

1. Refer [ZooKeeper tutorial](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html), compile ZooKeeper and run on 127.0.0.1:2181.
2. Get source code of chronos, compile through `mvn clean package -DskipTests`(Thrift required).
3. Enter the directory of chronos-server, execute `sh ./chronos.sh` to run ChronosServer.
4. Enter the directory of chronos-client, run `mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.client.ChronosClient" -Dexec.args="127.0.0.1:2181 default-cluster"`.

## Scenario

* Need globally strictly monotone increasing timestamp to implement global transation, like [Percolator](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36726.pdf).
* Need globally unique values. Unlike [snowflake](https://github.com/twitter/snowflake) which relies on NTP, chronos has more restricted constraint and handles failover by nature.

## Tools

* List_servers.rb is used to display the status of all running ChronosServers.
* Translate_timestamp.rb is used to translate the timestamp into world time.
* Process_benchmark_log.rb is used to process the log of benchmark program.

## Testing

* Performance Test

| Client Thread | Average QPS | Average Latency(ms) | Average Failover Time(s) | Server QPS |
|-------------|---------|------------------|---------------------|------------|
| 1      | 10792.757    | 0.093            | 3.056               | 32378.271  |
| 10     | 7919.679     | 0.127            | 3.053               | 237590.370 |
| 20     | 6676.801     | 0.164            | 3.952               | 400788.060 |
| 50     | 3954.026     | 0.255            | 4.044               | 593103.900 |
| 100    | 1791.251     | 0.605            | 5.470               | 537375.300 |
| 50(Optimum) | 3993.749 | 0.251           | 0.000               | 599062.350 |

* Failover Test

Continuously kill ChronosServer and ZooKeeper instance for one week, no correctness issue found and the failover time is in line with expectations.

Notice: **Failover time can be configured by tickTime of ZooKeeper and sessionTimeout of Chronos**. **Online service should use [Supervisor](http://supervisord.org/) or [God](http://godrb.com/) to pull up the ChronosServers after failover**.

