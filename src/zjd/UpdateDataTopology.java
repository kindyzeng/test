package zjd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.grid.Grid;
import com.grid.UGrid;
import scala.collection.mutable.Map;
import storm.kafka.*;
import test.DistributeSingleHGridIndexBolt;

import java.util.Arrays;

/**
 * Created by 金迪 on 2017/5/26.
 */
public class UpdateDataTopology {
    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("ID","POINT"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String line  = tuple.getString(0);
            if(line.split("\t").length>=2){
                String id = line.split("\t")[1];
            collector.emit(new Values(id,line));}
        }

    }

    private final BrokerHosts brokerHosts;

    public UpdateDataTopology() {
        brokerHosts = new ZkHosts("20.10.11.6:2181,20.10.11.7:2181,20.10.11.8:2181,20.10.11.9:2181,20.10.11.10:2181");
    }

    public StormTopology buildTopology(String topic) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,"","point1");
        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        topologyBuilder.setSpout("UpdateData",new KafkaSpout(spoutConfig),3);
        topologyBuilder.setBolt("Print",new UpdateDataTopology.PrinterBolt(),3).shuffleGrouping("UpdateData");
        topologyBuilder.setBolt("HGridIndex",  new DistributeSingleHGridIndexBolt(),3).setMaxTaskParallelism(50).fieldsGrouping("Print", new Fields("ID"));
//        topologyBuilder.setBolt("Socket",new SocketBolt(),1).shuffleGrouping("HGridIndex");
        return topologyBuilder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setNumWorkers(66);
        conf.setMaxTaskParallelism(99);
        conf.put(Config.NIMBUS_HOST, "20.10.11.6");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("20.10.11.6","20.10.11.7","20.10.11.8","20.10.11.9","20.10.11.10"));

        UpdateDataTopology updateDataTopology = new UpdateDataTopology();
        StormTopology stormTopology = updateDataTopology.buildTopology(args[1]);

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

//        Runnable runnable = new Runnable() {
//            public void run() {
//                while (true) {
//                    System.out.println("Hello !!");
//                    try {
//                        Thread.sleep(20*1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        };
//        Thread thread = new Thread(runnable);
//        thread.start();
    }
}
