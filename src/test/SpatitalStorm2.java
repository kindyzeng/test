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

import java.util.Arrays;

import storm_test.FilePointSpout;
import storm_test.FilePolygonSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SpatitalStorm2 {
	
	
	public static void main(String[] args) throws Exception {

//		String updateleftTopic = "updateleft4";
//		String updateRightTopic = "updateright4";
//		String queryTopic = "query1";
//		String zkRoot ="/storm";	
//		String spoutUpdateLeftId = "updateleft4";
//		String spoutUpdateRightId = "updateright4";
//		String spoutQueryId = "query";
//		BrokerHosts brokerHosts = new ZkHosts("202.121.180.101:2181,202.121.180.102:2181,202.121.180.103:2181"); 
//		SpoutConfig kafkaUpdateLeftConfig = new SpoutConfig(brokerHosts, updateleftTopic, zkRoot, spoutUpdateLeftId);
//		SpoutConfig kafkaUpdateRightConfig = new SpoutConfig(brokerHosts, updateRightTopic, zkRoot, spoutUpdateRightId);
//		SpoutConfig kafkaQueryConfig  =new SpoutConfig(brokerHosts, queryTopic, zkRoot, spoutQueryId);
//		kafkaUpdateLeftConfig.forceFromStart = true;		
//		kafkaQueryConfig.forceFromStart =true;
		

		Config conf = new Config();
//		
		conf.setNumWorkers(66);
		conf.setMaxTaskParallelism(99);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));
		//
		//
//		kafkaUpdateRightConfig.scheme =  new SchemeAsMultiScheme(new StringScheme());
//		kafkaUpdateLeftConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		kafkaQueryConfig.scheme = new SchemeAsMultiScheme(new StringScheme());



		TopologyBuilder builder = new TopologyBuilder();
//
//		builder.setSpout("UpdateLeftSpout", new KafkaSpout(kafkaUpdateLeftConfig), 28);
//		builder.setSpout("UpdateRightSpout", new KafkaSpout(kafkaUpdateRightConfig),28);
//		builder.setSpout("QuerySpout", new KafkaSpout(kafkaQueryConfig), 1);
		builder.setSpout("QuerySpout", new  FilePointSpout(args[7]), 1);
		builder.setSpout("UpdateLeftSpout", new FilePointSpout(args[4]),4);
		builder.setSpout("UpdateRightSpout", new FilePointSpout(args[6]),4);
		builder.setBolt("LeftUpdatePartition2", new UpdatePartition50(50), 6).shuffleGrouping("UpdateLeftSpout");
		builder.setBolt("RightUpdatePartition2", new UpdatePartition50(50),6).shuffleGrouping("UpdateRightSpout");	
		builder.setBolt("QueryPartition2", new QueryPartition50(), 2).shuffleGrouping("QuerySpout");
		builder.setBolt("RStreeIndex", new DistributeRtreeIndex(50,args[2],args[3]),30).setMaxTaskParallelism(50).fieldsGrouping("LeftUpdatePartition2", new Fields("GridIndex")).fieldsGrouping("QueryPartition2", new Fields("GridIndex")).fieldsGrouping("RightUpdatePartition2", new Fields("GridIndex"));
//		builder.setBolt("RtreeIndex", new DistributeRtreeIndex(50,args[2],args[3]),24).setMaxTaskParallelism(50).fieldsGrouping("LeftUpdatePartition", new Fields("GridIndex")).fieldsGrouping("RightUpdatePartition", new Fields("GridIndex"));
		//		builder.setBolt("SpatitalIndex", new SpatitalIndex(),1).fieldsGrouping("QueryPartition", new Fields("GridIndex"));
//		builder.setBolt("Output", new Output(args[0]),6).fieldsGrouping("SpatitalIndex", new Fields("StreamID"));
		conf.setDebug(true);

		if (args != null && args.length > 1) {
			conf.setNumWorkers(Integer.valueOf(args[0].toString()));
			StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
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
