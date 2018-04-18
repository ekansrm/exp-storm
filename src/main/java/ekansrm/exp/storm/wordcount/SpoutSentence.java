package ekansrm.exp.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * @author kami.wm
 */
public class SpoutSentence implements IRichSpout {
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
