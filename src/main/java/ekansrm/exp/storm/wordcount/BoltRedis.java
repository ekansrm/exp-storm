package ekansrm.exp.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BoltRedis {
  public int i = 0;
  public static Jedis jedis;
  public Map<String,String> map = new HashMap<String,String>();
  //jedis,生产环境最好用JedisPool
  static {
    jedis = new Jedis("192.168.1.201",6379);
    jedis.auth("cjqc123456");
  }

  public void execute(Tuple tuple) {
    String string = new String((byte[]) tuple.getValue(0));

    i++;
    String[] datas = string.split(" ");

    System.out.println("【收到消息：" + i + " 条数据】" + string);

    map.put("a", UUID.randomUUID()+ "_" + string);
    map.put("b", UUID.randomUUID()+ "_" + string);
    map.put("c", UUID.randomUUID()+ "_" + string);
    map.put("d", UUID.randomUUID()+ "_" + string);

    jedis.hmset("test", map);
  }

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    // 初始化
  }

  public void declareOutputFields(OutputFieldsDeclarer arg0) {

  }

}
