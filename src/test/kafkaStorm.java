package test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import storm.kafka.Broker;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class kafkaStorm {
	private static String abc;
	public static class WordSpliter extends BaseBasicBolt {
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			System.out.println("fuck");
			String line = input.getString(0);
			String[] words = line.split(" ");
			for (String word : words) {
				word = word.trim();
				if (org.apache.commons.lang.StringUtils.isNotBlank(word)) {
					word = word.toLowerCase();
					collector.emit(new Values(word));
				}
			}
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		} 
	}


	public static class WordCount extends BaseBasicBolt {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		String output2;

		public WordCount(String output2) {
			super();
			this.output2 = output2;
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			File file =new File(output2);
		
				try {
					if(!file.exists()){
					file.createNewFile();
					FileWriter fileWritter = new FileWriter(file.getName(),true);
					BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
					bufferWritter.write("key is "+word+"count is "+count);
					System.out.println("key is "+word+"count is "+count);
					bufferWritter.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block 
					e.printStackTrace();
				}

		
			System.out.println("Done");

			//if file doesnt exists, then create it


			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static void main(String[] args) throws Exception {


		
		System.out.println("1zy123");
		
		String topic = "zy1";
		String zkRoot ="/storm";
		
		String spoutId = "zytest";
		BrokerHosts brokerHosts = new ZkHosts("202.121.180.101:2181,202.121.180.102:2181,202.121.180.103:2181"); 
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		kafkaConfig.forceFromStart = true;
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(5);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));
		
		
//        GlobalPartitionInformation hostsAndPartitions = new GlobalPartitionInformation();
//        hostsAndPartitions.addPartition(0, new Broker("202.121.180.100", 9092));
//        hostsAndPartitions.addPartition(0, new Broker("202.121.180.101", 9092));
//        hostsAndPartitions.addPartition(0, new Broker("202.121.180.102", 9092));
//        BrokerHosts brokerHosts = new StaticHosts(hostsAndPartitions);
//        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, "storm");
		
       kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		//		String time=format.format(date);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(kafkaConfig), 4);
		builder.setBolt("split", new WordSpliter(), 8).shuffleGrouping("spout");
		builder.setBolt("count", new WordCount(args[0]), 12).fieldsGrouping("split", new Fields("word"));
		
		conf.setDebug(true);

		if (args != null && args.length > 1) {
			conf.setNumWorkers(Integer.valueOf(args[1].toString()));
			StormSubmitter.submitTopologyWithProgressBar(args[2], conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", conf, builder.createTopology());
//			Thread.sleep(100000);
//			cluster.shutdown();
		}
	}
}
