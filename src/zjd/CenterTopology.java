package zjd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;
import test.DistributeHashTableIndex;
import test.DistributeSingleHGridIndexBolt;

import java.util.Arrays;

/**
 * Created by 金迪 on 2017/5/31.
 */
public class CenterTopology {
    private final BrokerHosts brokerHosts;

    public CenterTopology() {
        brokerHosts = new ZkHosts("202.121.180.3:2181,202.121.180.4:2181,202.121.180.5:2181,202.121.180.6:2181,202.121.180.7:2181");
    }

    public StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"point","","point");
        spoutConfig.forceFromStart = false;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        topologyBuilder.setSpout("DataSpout",new KafkaSpout(spoutConfig),3);
        topologyBuilder.setBolt("HGridIndex",new DistributeSingleHGridIndexBolt(),5).setMaxTaskParallelism(50).shuffleGrouping("DataSpout");

//        topologyBuilder.setSpout("UpdateData",new KafkaSpout(spoutConfig),3);
//        topologyBuilder.setBolt("Print",new UpdateDataTopology.PrinterBolt(),3).shuffleGrouping("UpdateData");
//        topologyBuilder.setBolt("HGridIndex",  new DistributeSingleHGridIndexBolt(),5).setMaxTaskParallelism(50).fieldsGrouping("Print", new Fields("ID"));
        return topologyBuilder.createTopology();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setNumWorkers(66);
        conf.setMaxTaskParallelism(99);
        conf.put(Config.NIMBUS_HOST, "202.121.180.3");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.4","202.121.180.5","202.121.180.6","202.121.180.7"));

        CenterTopology centerTopology = new CenterTopology();
        StormTopology stormTopology = centerTopology.buildTopology();

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, stormTopology);
        }
        else {
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, stormTopology);
            //			Thread.sleep(100000);
            //			cluster.shutdown();
        }

    }
}
