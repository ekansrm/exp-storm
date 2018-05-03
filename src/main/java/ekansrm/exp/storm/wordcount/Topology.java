package ekansrm.exp.storm.wordcount;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

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
    String zkRoot = "";

    String id = "storm-word-count-test";

    ZkHosts zkHosts = new ZkHosts(zks);
    SpoutConfig kafkaSpout = new SpoutConfig(zkHosts, topic, zkRoot, id);
    kafkaSpout.scheme = new SchemeAsMultiScheme(new StringScheme());

    TopologyBuilder builder = new TopologyBuilder();

    // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
    builder.setSpout("kafka-reader", new KafkaSpout(kafkaSpout), 1);
    builder.setBolt("word-splitter", new BoltSplitSentence(), 1).shuffleGrouping("kafka-reader");
    builder.setBolt("word-counter", new BoltWordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

    return builder;

  }

  static public TopologyBuilder redisWordCountBuilder() {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("sentence", new SpoutSentence(),1);
    builder.setBolt("word-splitter", new BoltSplitSentence(), 1).shuffleGrouping("sentence");
    builder.setBolt("word-counter", new BoltWordCounter()).fieldsGrouping("word-splitter", new Fields("word"));
    return builder;

  }

}
