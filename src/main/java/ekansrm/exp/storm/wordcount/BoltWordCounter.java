package ekansrm.exp.storm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kami.wm
 */
public class BoltWordCounter implements IRichBolt {
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
