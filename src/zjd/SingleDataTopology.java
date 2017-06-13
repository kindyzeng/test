package zjd;

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
import scala.util.parsing.combinator.testing.Str;
import storm.kafka.*;
import storm_test.SingleDataSetQuerySpout;
import storm_test.SingleDataSetSpout;
import test.AggerateBolt;
import test.DistributeSingleHGridIndexBolt;
import test.DistributeSingleRtreeIndexBolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SingleDataTopology {

	public static class PrinterBolt extends BaseBasicBolt {
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("ID","POINT"));
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String line  = tuple.getString(0);
			String id = line.split("\t")[1];
			collector.emit(new Values(id,line));
		}

	}
	
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
//		
		conf.setNumWorkers(66);
		conf.setMaxTaskParallelism(99);
		conf.put(Config.NIMBUS_HOST, "192.168.1.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("192.168.1.101","192.168.1.102"));


		//KafkaSpout
		BrokerHosts brokerHosts = new ZkHosts("192.168.1.100:2181,192.168.1.101:2181,192.168.1.102:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"zjdt","","zjd");
		spoutConfig.forceFromStart = true;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("UpdateData",new KafkaSpout(spoutConfig),3);
//		builder.setBolt("print",new PrinterBolt(),3).shuffleGrouping("KafkaData");

		SpoutConfig config = new SpoutConfig(brokerHosts,"zjdq","","zjdq");
		config.forceFromStart = true;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		builder.setSpout("QuerySpout", new KafkaSpout(config), 3);
//		builder.setSpout("UpdateSpout", new SingleDataSetSpout(args[3]),7);

		builder.setBolt("Print",new PrinterBolt(),1).shuffleGrouping("UpdateData");
		builder.setBolt("HGridIndex", new DistributeSingleHGridIndexBolt(),5).setMaxTaskParallelism(50).fieldsGrouping("Print", new Fields("ID")).allGrouping("QuerySpout");
//		builder.setBolt("Aggerate", new AggerateBolt(),3).fieldsGrouping("HGridIndex", new Fields("QueryID"));
		
		conf.setDebug(true);conf.setDebug(true);

		if (args != null && args.length > 0) {
//			conf.setNumWorkers(Integer.valueOf(args[0].toString()));
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
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
