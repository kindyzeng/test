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

import storm_test.SingleDataSetQuerySpout;
import storm_test.SingleDataSetSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SingleDataSetTopology {
	
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
//		
		conf.setNumWorkers(66);
		conf.setMaxTaskParallelism(99);
		conf.put(Config.NIMBUS_HOST, "202.121.180.85");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.82","202.121.180.83"));



		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("QuerySpout", new  SingleDataSetQuerySpout (args[2]), 1);
		builder.setSpout("UpdateSpout", new SingleDataSetSpout(args[3]),7);		
		builder.setBolt("RStreeIndex", new  DistributeSingleRtreeIndexBolt(),5).setMaxTaskParallelism(50).fieldsGrouping("UpdateSpout", new Fields("ID")).allGrouping("QuerySpout");
		builder.setBolt("Aggerate", new AggerateBolt(),3).fieldsGrouping("RStreeIndex", new Fields("QueryID"));	
		
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
