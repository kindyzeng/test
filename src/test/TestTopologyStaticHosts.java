package test;

import java.util.Arrays;

import storm.kafka.Broker;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TestTopologyStaticHosts {


    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }

    }

    public static void main(String[] args) throws Exception {
    	
        GlobalPartitionInformation hostsAndPartitions = new GlobalPartitionInformation();
        hostsAndPartitions.addPartition(0, new Broker("202.121.180.100", 9092));
        hostsAndPartitions.addPartition(0, new Broker("202.121.180.101", 9092));
        hostsAndPartitions.addPartition(0, new Broker("202.121.180.102", 9092));
        BrokerHosts brokerHosts = new StaticHosts(hostsAndPartitions);
        
        

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "zy1", "/tmp/dev-storm-zookeeper", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
    	Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(5);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));
        cluster.submitTopology("kafka-test", config, builder.createTopology());
    }
}
