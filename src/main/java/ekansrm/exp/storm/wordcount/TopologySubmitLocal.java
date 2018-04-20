package ekansrm.exp.storm.wordcount;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.Utils;

/**
 *
 * @author kami.wm
 */
public class TopologySubmitLocal {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

    Config config = new Config();
    config.setDebug(true);

    if(args!=null && args.length>0) {
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], config, Topology.redisWordCountBuilder().createTopology());
    } else {
      //创建一个本地模式cluster
      LocalCluster cluster = new LocalCluster();
      
      System.out.println("start word count");
      cluster.submitTopology("word count", config, Topology.redisWordCountBuilder().createTopology());
      
      //运行60s
      Utils.sleep(60000);

      //终止topology任务
      System.out.println("begin kill topology");
      cluster.killTopology("word count");
      cluster.shutdown();
    }
  }

}

