package ekansrm.exp.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


/**
 *
 * @author kami.wm
 */
public class WordCount {

  public static class WordProduct implements IRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    Random rand;

    /**
     * 这是第一个方法要执行的方法（只执行一次），常用于初始化的（如打开文件、打开连接等）
     * 里面接收了三个参数，第一个是创建Topology时的配置，
     * 第二个是所有的Topology数据，第三个是用来把Spout的数据发射给bolt
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      System.out.println("in open()*****************");
      this.collector = collector;
      rand = new Random();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
      // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
      // TODO Auto-generated method stub

    }

    /**
     * 这个方法会不断被调用，为了降低它对CPU的消耗，当任务完成时让它sleep一下
     */
    @Override
    public void nextTuple() {
      Utils.sleep(10000);
      String[] sentences = new String[] {
        "this is a test",
        "hello world",
        "I am fine",
        "china duan"
      };
      String s = sentences[rand.nextInt(sentences.length)];

      System.out.println("in nextTuple():" + s);
      collector.emit(new Values(s));
    }

    @Override
    public void ack(Object msgId) {
      System.out.println("OK:" + msgId);
    }

    @Override
    public void fail(Object msgId) {
      System.out.println("FAIL:" + msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      // TODO Auto-generated method stub
      return null;
    }

  }
  public static class SplitSentence implements IRichBolt {
    private OutputCollector collector;

    /**
     * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
     * 因为这只是个Demo，我们用它来打印我们的计数器
     */
    @Override
    public void cleanup() {
      System.out.println("cleanup()");
    }

    /**
     * 这是bolt中最重要的方法，每当接收到一个tuple时，此方法便被调用
     * 这个方法的作用就是把文本文件中的每一行切分成一个个单词，并把这些单词发射出去（给下一个bolt处理）
     */
    @Override
    public void execute(Tuple input) {
      String s = input.getString(0);
      String[] words = s.split(" ");
      for(String w : words) {
        w = w.trim();
        if(!w.isEmpty()) {
          collector.emit(new Values(w));
        }
      }
      //确认成功处理一个tuple
      collector.ack(input);
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
      this.collector = arg2;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      // TODO Auto-generated method stub
      return null;
    }

  }
  public static class WordCounter implements IRichBolt {
    private OutputCollector collector;
    Map<String,Integer> counts = new HashMap<String, Integer>();

    /**
     * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
     * 因为这只是个Demo，我们用它来打印我们的计数器
     */
    @Override
    public void cleanup() {
      System.out.println("cleanup()");
    }

    /**
     * 这是bolt中最重要的方法，每当接收到一个tuple时，此方法便被调用
     * 这个方法的作用就是统计单词次数
     */
    @Override
    public void execute(Tuple input) {
      String w = input.getString(0);
      Integer count = counts.get(w);
      if(count==null) {
        count = 1;
      } else {
        count++;
      }
      counts.put(w, count);
      collector.emit(new Values("word",count));

      System.out.println(w + ":" + Integer.toString(count));
      //确认成功处理一个tuple
      collector.ack(input);
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
      this.collector = arg2;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      // TODO Auto-generated method stub
      return null;
    }

  }

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    //定义一个Topology
    TopologyBuilder builder = new TopologyBuilder();

    //为topology设置spout(节点)
    builder.setSpout("input", new WordProduct(),1);

    //为topology设置bolt(节点)
    builder.setBolt("bolt_sentence", new SplitSentence(),1).shuffleGrouping("input");
    builder.setBolt("bolt_wordcounter", new WordCounter(),1).fieldsGrouping("bolt_sentence", new Fields("word"));

    //配置
    Config config = new Config();
    //TOPOLOGY_DEBUG(setDebug), 当它被设置成true的话， storm会记录下每个组件所发射的每条消息。这在本地环境调试topology很有用， 但是在线上这么做的话会影响性能的。
    config.setDebug(true);
    //定义你希望集群分配多少个工作进程给你来执行这个topology.
    config.setNumWorkers(2);

    if(args!=null && args.length>0) {
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } else {
      //创建一个本地模式cluster
      LocalCluster cluster = new LocalCluster();
      System.out.println("start word count");
      cluster.submitTopology("word count", config, builder.createTopology());
      //运行60s
      Utils.sleep(60000);

      //终止topology任务
      System.out.println("begin kill topology");
      cluster.killTopology("word count");
      cluster.shutdown();
    }
  }

}

