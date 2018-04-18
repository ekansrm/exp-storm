package ekansrm.exp.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;


/**
 *
 * @author kami.wm
 */
public class TopologySubmitRemote {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

    //配置
    Config config = new Config();

    config.setDebug(true);
    config.setNumWorkers(3);

    //配置nimbus连接主机地址，比如：192.168.10.1
    config.put(Config.NIMBUS_HOST, "127.0.0.1");

    //配置nimbus连接端口，默认 6627
    config.put(Config.NIMBUS_THRIFT_PORT, 6627);

//    //配置zookeeper连接主机地址，可以使用集合存放多个
//    config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS));
//    //配置zookeeper连接端口，默认2181
//    config.put(Config.STORM_ZOOKEEPER_PORT,STORM_ZOOKEEPER_PORT);

    
    // 设置系统属性 storm.jar, 指向jar包的位置
    System.setProperty("storm.jar", "target/exp.storm-1.0-SNAPSHOT.jar");

    StormSubmitter.submitTopology("test", config, Topology.simpleWordCountBuilder().createTopology());

  }

}

