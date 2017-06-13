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
package storm_test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import model.Point_Mdl;
import model.WordCountTest;

import org.apache.commons.codec.binary.StringUtils;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
	private static String abc;
	private static SpoutConfig kafkaUpdateConfig;
	private static SpoutConfig kafkaQueryConfig;
	public static class WordSpliter extends BaseBasicBolt {
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
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
		Map<String, WordCountTest> counts = new HashMap<String, WordCountTest>();
		
        private String _path;
		public WordCount(String path) {
			super();
			_path = path;
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {

			if(tuple.getSourceComponent().equalsIgnoreCase("split1"))
			{
				String word = tuple.getString(0);
				WordCountTest point  = counts.get(word);

				if (point == null)
				{
					point = new WordCountTest(word, 1);		
				}
				point.setCount(point.getCount()+1);
				counts.put(word, point);
			

			}
			else {
				String word = tuple.getString(0);
				WordCountTest point  = counts.get(word);
				String output ;
				if(point!=null)
				{
					output= "word is"+word+" "+"count is"+point.getCount();
				}
				else
				{
					output ="word is"+word+" "+"count is 0";
				}
				FileWriter fw = null; 
				try {	
					fw = new FileWriter(_path,true);
					fw.write(output); 
					fw.close();   
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}		
	}
	
	public void init()
	{
		System.out.print("fuck");
		String updateTopic = "wdupdate";
		String queryTopic = "wdquery";
		String zkRoot ="/storm";	
		String spoutUpdateId = "update";
		String spoutQueryId = "query";
		BrokerHosts brokerHosts = new ZkHosts("202.121.180.101:2181,202.121.180.102:2181,202.121.180.103:2181"); 
		 kafkaUpdateConfig = new SpoutConfig(brokerHosts, updateTopic, zkRoot, spoutUpdateId);
		 kafkaQueryConfig  =new SpoutConfig(brokerHosts, queryTopic, zkRoot, spoutQueryId);
		kafkaUpdateConfig.forceFromStart = true;
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(5);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));
		kafkaUpdateConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaQueryConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();		
		builder.setSpout("UpdateSpout", new KafkaSpout(kafkaUpdateConfig), 6);
		builder.setSpout("QuerySpout", new KafkaSpout(kafkaQueryConfig), 1);		
		builder.setBolt("split1", new WordSpliter(), 8).shuffleGrouping("QuerySpout");
		builder.setBolt("split2", new WordSpliter(), 1).shuffleGrouping("UpdateSpout");
		builder.setBolt("count", new WordCount(args[0]), 12).fieldsGrouping("split1", new Fields("word")).fieldsGrouping("split2", new Fields("word"));
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 1) {
			conf.setNumWorkers(Integer.valueOf(args[1].toString()));
			StormSubmitter.submitTopologyWithProgressBar(args[2], conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", conf, builder.createTopology());
			Thread.sleep(100000);
			//			cluster.shutdown();
		}
	}
}
