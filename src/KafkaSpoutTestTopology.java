import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class KafkaSpoutTestTopology {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestTopology.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            BufferedWriter out = null;
            try {
                FileWriter fileWriter = new FileWriter("/usr/local/software/2.txt",true);
                out = new BufferedWriter(fileWriter);
                out.write("12313123");
                out.flush(); // 把缓存区内容压入文件
                out.close(); // 最后记得关闭文件
            } catch (IOException e) {
                e.printStackTrace();
            }

            LOG.info(tuple.toString());
        }

    }

    private final BrokerHosts brokerHosts;

    public KafkaSpoutTestTopology() {
        brokerHosts = new ZkHosts("202.121.180.85:2181,202.121.180.82:2181,202.121.180.83:2181");
    }

    public StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"zy","","zjd");
        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        topologyBuilder.setSpout("words",new KafkaSpout(spoutConfig),3);
        topologyBuilder.setBolt("print",new PrinterBolt(),3).shuffleGrouping("words");
        return topologyBuilder.createTopology();

//        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "zy", "","storm");
//        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("words", new KafkaSpout(kafkaConfig), 8);
//        builder.setBolt("print", new PrinterBolt(),3).shuffleGrouping("words");
//        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        File file = new File("/usr/local/software/2.txt");
        if(!file.exists()){
            file.setWritable(true,false);
            file.createNewFile();
        }

//        String kafkaZk = args[0];
        KafkaSpoutTestTopology kafkaSpoutTestTopology = new KafkaSpoutTestTopology();
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 0) {
            String name = args[0];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(10);
            config.put(Config.NIMBUS_HOST, "202.121.180.85");
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.85","202.121.180.82","202.121.180.83"));
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            //config.put(Config.NIMBUS_HOST, "202.121.180.85");
            //config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            //config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            //config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.85","202.121.180.82","202.121.180.83"));
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }
}
