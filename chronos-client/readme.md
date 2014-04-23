# Chronos-client

## 简介
Chronos-client是chronos服务的客户端。使用chronos-client可以快速获得一个或者多个**全局严格单调递增的timestamp**，而且它能处理服务端failover的情况，不管发生何种故障，chronos-client都能通过ZooKeeper得到当前的主ChronosServer继续请求服务。

## Maven依赖
* 如果你的项目已经使用thrift-0.8.0，请添加以下依赖。

    ```
    <dependency>
      <groupId>com.xiaomi.infra</groupId>
      <artifactId>chronos-client</artifactId>
      <version>1.2.0-thrift0.8.0</version>
    </dependency>
    ```

* 如果你的项目已经使用thrift-0.5.0，请添加以下依赖。

    ```
    <dependency>
      <groupId>com.xiaomi.infra</groupId>
      <artifactId>chronos-client</artifactId>
      <version>1.2.0-thrift0.5.0</version>
    </dependency>
    ```

## 使用实例
Chronos-client提供了`getTimestamp()`和`getTimestamps(int)`两个接口，客户端一次可以请求一个或者多个timestamp，请求的timestamp会在服务端记录，保证每次请求得到的值都比前一次得到的要大。

    import com.xiaomi.infra.chronos.client.ChronosClient;
    
    public class ChronosClientDemo {
      public static void main(String[] args) throws Exception{
        ChronosClient chronosClient = new ChronosClient("127.0.0.1:2181", "default-cluster");
        long timestamp = chronosClient.getTimestamp(); // 366379383866785792
        long timestamps = chronosClient.getTimestamps(100); // 366379383867310080
      }
    }

或者在项目根目录执行`mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.client.ChronosClient" -Dexec.args="$zkQuorum $clusterName"`直接调用。

## 源码编译
1. 进入chronos-client项目，执行`mvn clean packge -DskipTest`编译。
2. 编译时会使用maven-thrift-plugin插件，需要调用本地thrift命令生成代码。
3. 要部署不同thrift版本的chronos-client，需要修改pom.xml里面插件的参数。

---

# Chronos-client

## Introduction
Chronos-client is the client of chronos service. With chronos-client, you can get a **globally strictly monotone increasing timestamp** easily. It also handles failover problem. Once the master ChronosServer fails, this client can reconnect with the new master server through ZooKeeper and keep on requesting.

## Maven Dependency
* If you're already using thrift-0.8.0, add this in your pom.xml.

    ```
    <dependency>
      <groupId>com.xiaomi.infra</groupId>
      <artifactId>chronos-client</artifactId>
      <version>1.2.0-thrift0.8.0</version>
    </dependency>
    ```

* If you're already using thrift-0.5.0, add this in your pom.xml.

    ```
    <dependency>
      <groupId>com.xiaomi.infra</groupId>
      <artifactId>chronos-client</artifactId>
      <version>1.2.0-thrift0.5.0</version>
    </dependency>
    ```

## Usage
Chronos-client provides two API, `getTimestamp()` and `getTimestamps(int)`. Client can request one or many timestamps for each RPC request. The allocated timestamp is persistent in server(actually in ZooKeeper), and we can guarantee that all the timestamps are globally strictly monotone increasing.

    import com.xiaomi.infra.chronos.client.ChronosClient;
    
    public class ChronosClientDemo {
      public static void main(String[] args) throws Exception{
        ChronosClient chronosClient = new ChronosClient("127.0.0.1:2181", "default-cluster");
        long timestamp = chronosClient.getTimestamp(); // 366379383866785792
        long timestamps = chronosClient.getTimestamps(100); // 366379383867310080
      }
    }

Or just execute `mvn exec:java -Dexec.mainClass="com.xiaomi.infra.chronos.client.ChronosClient" -Dexec.args="$zkQuorum $clusterName"` in command-line.

## Compile Source
1. Enter the directory of chronos-client, compile with `mvn clean packge -DskipTest`.
2. Make sure you have installed Thrift because it uses maven-thrift-plugin to generate code. 
3. If you want to use different version of Thrift, change parameters of that plugin in pom.xml.

