package ekansrm.exp.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;


/**
 *
 * @author kami.wm
 */
public class TopologySubmitLocal {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

    Config config = new Config();
    config.setDebug(true);

    if(args!=null && args.length>0) {
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], config, Topology.simpleWordCountBuilder().createTopology());
    } else {
      //创建一个本地模式cluster
      LocalCluster cluster = new LocalCluster();
      
      System.out.println("start word count");
      cluster.submitTopology("word count", config, Topology.simpleWordCountBuilder().createTopology());
      
      //运行60s
      Utils.sleep(60000);

      //终止topology任务
      System.out.println("begin kill topology");
      cluster.killTopology("word count");
      cluster.shutdown();
    }
  }

}

