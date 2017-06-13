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

import storm_test.FilePolygonSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SpatitalStormHash {
	
	
	public static void main(String[] args) throws Exception {


		Config conf = new Config();
//		
		conf.setNumWorkers(66);
		conf.setMaxTaskParallelism(99);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));




		TopologyBuilder builder = new TopologyBuilder();


		builder.setSpout("QuerySpout", new  FilePolygonSpout(args[7],args[5]), 1);
		builder.setSpout("UpdateLeftSpout", new FilePolygonSpout(args[4],args[5]),1);
		builder.setSpout("UpdateRightSpout", new FilePolygonSpout(args[6],args[5]),1);
		builder.setBolt("LeftUpdatePartition", new UpdatePartition50(50), 2).shuffleGrouping("UpdateLeftSpout");
		builder.setBolt("RightUpdatePartition", new UpdatePartition50(50), 2).shuffleGrouping("UpdateRightSpout");	
		builder.setBolt("QueryPartition", new QueryPartition50(), 2).shuffleGrouping("QuerySpout");
		builder.setBolt("HashTableIndex", new DistributeHashTableIndex(50.0),35).setMaxTaskParallelism(50).fieldsGrouping("LeftUpdatePartition", new Fields("GridIndex")).fieldsGrouping("QueryPartition", new Fields("GridIndex")).fieldsGrouping("RightUpdatePartition", new Fields("GridIndex"));


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
