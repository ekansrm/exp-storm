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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    config.put(Config.NIMBUS_SEEDS, Collections.singletonList("127.0.0.1"));

    //配置nimbus连接端口，默认 6627
    config.put(Config.NIMBUS_THRIFT_PORT, 6627);
    config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("172.22.0.6", "172.22.0.5", "172.22.0.9"));
    config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

    Map<String, String> map =new HashMap<>();
    // 配置Kafka broker地址
    map.put("metadata.broker.list", "172.22.0.2:9092,172.22.0.3:9092,172.22.0.4:9092");
    // serializer.class为消息的序列化类
    map.put("serializer.class", "kafka.serializer.StringEncoder");

    config.put("kafka.broker.properties", map);

    JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost("storm-redis").setPort(6379).build();
    RedisStoreMapper storeMapper = new WordCountStoreMapper();
    RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

    TopologyBuilder builder = Topology.kafkaWordCountBuilder();

    builder.setBolt("redis", storeBolt).shuffleGrouping("word-counter");

    
    // 设置系统属性 storm.jar, 指向jar包的位置
    System.setProperty("storm.jar", "target/exp.storm-1.0-SNAPSHOT.jar");

    StormSubmitter.submitTopology("kafka-redis", config, builder.createTopology());

  }

}

