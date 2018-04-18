package ekansrm.exp.storm.wordcount;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

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

}
