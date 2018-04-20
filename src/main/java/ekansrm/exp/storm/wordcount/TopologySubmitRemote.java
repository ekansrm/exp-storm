package ekansrm.exp.storm.wordcount;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Collections;

/**
 *
 * @author kami.wm
 */
public class TopologySubmitRemote {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

    //配置
    Config config = new Config();

    config.setDebug(true);
    config.setNumWorkers(3);

    //配置nimbus连接主机地址，比如：192.168.10.1
    config.put(Config.NIMBUS_SEEDS, Collections.singletonList("localhost"));

    //配置nimbus连接端口，默认 6627
    config.put(Config.NIMBUS_THRIFT_PORT, 6627);

    JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
      .setHost("172.22.0.14").setPort(6379).build();
    RedisStoreMapper storeMapper = new WordCountStoreMapper();
    RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

//    //配置zookeeper连接主机地址，可以使用集合存放多个
//    config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS));
//    //配置zookeeper连接端口，默认2181
//    config.put(Config.STORM_ZOOKEEPER_PORT,STORM_ZOOKEEPER_PORT);

    TopologyBuilder builder = Topology.redisWordCountBuilder();

    builder.setBolt("redis", storeBolt).shuffleGrouping("word-counter");

    
    // 设置系统属性 storm.jar, 指向jar包的位置
    System.setProperty("storm.jar", "target/exp.storm-1.0-SNAPSHOT.jar");

    StormSubmitter.submitTopology("test", config, builder.createTopology());

  }

}

