package ekansrm.exp.storm.wordcount;

import org.apache.storm.kafka.*;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * @author kami.wm
 */
public class Topology {

  static public TopologyBuilder simpleWordCountBuilder() {
    //定义一个Topology
    TopologyBuilder builder = new TopologyBuilder();

    //为topology设置spout(节点)
    builder.setSpout("input", new SpoutSentence(),1);

    //为topology设置bolt(节点)
    builder
      .setBolt("bolt_sentence", new BoltSplitSentence(),1)
      .shuffleGrouping("input");

    builder
      .setBolt("bolt_wordcounter", new BoltWordCounter(),1)
      .fieldsGrouping("bolt_sentence", new Fields("word"));

    return builder;

  }

  static public TopologyBuilder kafkaWordCountBuilder() {
    String zks = "zk1.cloud:2181,zk2.cloud:2181,zk3.cloud:2181";
    String topic = "storm-word-count";

    // default zookeeper root configuration for storm
    String zkRoot = "/storm";

    String id = "word";

    BrokerHosts brokerHosts = new ZkHosts(zks);
    SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConf.zkServers = Arrays.asList("zk1.cloud", "zk2.cloud2", "zk3.cloud");
    spoutConf.zkPort = 2181;

    TopologyBuilder builder = new TopologyBuilder();

    // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
    builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 1);
    builder.setBolt("word-splitter", new BoltSplitSentence(), 1).shuffleGrouping("kafka-reader");
    builder.setBolt("word-counter", new BoltWordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

    return builder;

  }

  static public TopologyBuilder redisWordCountBuilder() {

    JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
      .setHost("127.0.0.1").setPort(6379).build();
    RedisStoreMapper storeMapper = new WordCountStoreMapper();
    RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("sentence", new SpoutSentence(),1);
    builder.setBolt("word-splitter", new BoltSplitSentence(), 1).shuffleGrouping("sentence");
    builder.setBolt("word-counter", new BoltWordCounter()).fieldsGrouping("word-splitter", new Fields("word"));
    return builder;

  }

}
