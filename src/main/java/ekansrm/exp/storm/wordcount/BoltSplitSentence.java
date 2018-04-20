package ekansrm.exp.storm.wordcount;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author kami.wm
 */
public class BoltSplitSentence implements IRichBolt {
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
