# Chronos-server

## 简介

Chronos-server是chronos的服务端。

使用chronos-server可以为客户端提供一个或者多个**全局严格单调递增的timestamp**，ChronosServer通过ZooKeeper实现了failover机制，任何一台服务器挂了也不会影响整个集群的服务。

## 使用

1. 进入chronos-server目录，通过`mvn clean package -DskipTests`编译源码。
2. 进入target里面的conf目录，编辑chronos.conf，填写依赖的ZooKeeper配置。
3. 进入target里面的bin目录，执行`sh ./chronos.sh`既可运行ChronosServer。

## 配置

默认的配置文件chronos.cfg如下。

<pre>
# zookeeper
zkQuorum=127.0.0.1:2181
clusterName=default-cluster
sessionTimeout=3000
connectRetryTimes=10

# secure
zkSecure=false
zkAdmin=h_chronos_admin
jaasFile=../conf/jaas.conf
krb5File=/etc/krb5.conf

# thrift
serverHost=127.0.0.1
serverPort=7911
maxThread=10000

# timestamp
# 262144000(2**18 * 1000) for one second
zkAdvanceTimestamp = 26214400000
</pre>

### ZooKeeper

设置zkQuorum可以指定你所依赖的ZooKeeper集群。
设置clusterName可以区分不同服务的chronos集群。
设置sessionTimeout可以指定与ZooKeeper的连接时间(**注意这个值必须大于两倍的tickTime**)。
设置connectRetryTimes可以指定与ZooKeeper的重连次数。

### Secure

设置zkSecure为true表示使用具有权限控制的ZooKeeper安全集群。
如果使用ZooKeeper安全集群，那么需要在这里指定admin用户名。
如果使用ZooKeeper安全集群，那么需要在这里提供jaas文件路径。
如果使用ZooKeeper安全集群，那么需要在这里提供krb5文件路径。

### Thrift

设置serverHost可以指定Thrift服务器的地址，一般设置为本机IP。
设置serverPort可以指定Thrift服务器的端口，只要与其他服务不冲突即可。
设置maxThread可以指定Thrift服务器的最大连接线程数，保持默认值即可。

### Timestamp

设置zkAdvanceTimestamp可以在ZooKeeper记录一个超前值，这个值越大访问ZooKeeper的次数就越少。

---

# Chronos-server

## Introduction
Chronos-server is the server of chronos service. 

Chronos-server can provide **globally strictly monotone increasing timestamp** for clients. ChronosServer has implemented failover mechanism so that the crash of master server will not bring down the service.

## Usage

1. Enter the directory of chronos-server, compile through `mvn clean package -DskipTests`.
2. Enter the directory of conf, edit chronos.conf and fill in the ZooKeeper you're using.
3. Enter the directory of bin, execute `sh ./chronos.sh` to monitor the running status.

## Configuation

Here is the default configuration file, chronos.cfg.

<pre>
# zookeeper
zkQuorum=127.0.0.1:2181
clusterName=default-cluster
sessionTimeout=3000
connectRetryTimes=10

# secure
zkSecure=false
zkAdmin=h_chronos_admin
jaasFile=../conf/jaas.conf
krb5File=/etc/krb5.conf

# thrift
serverHost=127.0.0.1
serverPort=7911
maxThread=10000

# timestamp
# 262144000(2**18 * 1000) for one second
zkAdvanceTimestamp = 26214400000
</pre>

### ZooKeeper

Set zkQuorum to indicate the dependent ZooKeeper cluster.
Set clusterName to distinguish between different chronos cluster.
Set sessionTimeout of ZooKeeper client(**Make sure it's bigger than twice of tickTime**).
Set connectRetryTimes of ZooKeeper client.

### Secure

Set zkSecure as true to use security ZooKeeper.
If zkSecure is true, then you should set admin name.
If zkSecure is true, then you should set jaas file.
If zkSecure is true, then you should set krb5 file.

### Thrift

Set serverHost to indicate the addrees of Thrift server.
Set serverPort to indicate the port of Thrift server.
Set maxThread to indicate the max number of threads of Thrift server.


### Timestamp

Set zkAdvanceTimestamp to record a advanced timestamp in ZooKeeper. The bigger this value is, the less times it will communicate with ZooKeeper.


