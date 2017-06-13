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

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.SingleJoinBolt;
import storm.starter.trident.TridentWordCount.Split;
import storm_test.WordCountTopology.WordSpliter;

public class WordJoin {
	
	 public static class WordSpliter extends BaseBasicBolt {
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String line = input.getString(0);
			String[] words = line.split(" ");
			for (String word : words) {
				word = word.trim();
				if (org.apache.commons.lang.StringUtils.isNotBlank(word)) {
					word = word.toLowerCase();
					collector.emit(new Values(word,"count"));
				}
			}
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","operate"));
		} 
	}
	 
	public static class bolt  extends BaseBasicBolt {
		String output2;
	    Map<String, Integer> counts = new HashMap<String, Integer>();
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			
			String word =  input.getString(0);
			String operate = input.getString(1);
			boolean isupda;
			String path = output2 + "\\"+  System.currentTimeMillis()+".txt";
			if(operate=="count")
			{
			     Integer count = counts.get(word);
			     if (count == null)
			        count = 0;
			      count++;
			      counts.put(word, count);			
			}
			else
			{			
				FileWriter fileWriter;
				Integer count = counts.get(word);
				  if (count == null)
				        count = 0;
				try {
					fileWriter = new FileWriter(path, true);
					fileWriter.write("word is "+ word + " count is "+count.toString()+"\n");
					fileWriter.flush();
					fileWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			collector.emit(new Values(word));
		}
		public bolt(String output2) {
			super();
			this.output2 = output2;
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		} 
	}


	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("count", new FileSentenceSpout("G:\\input1",args[0]), 5);
		builder.setSpout("query", new FileQuerySpout("G:\\input2",args[0]), 5);	
		builder.setBolt("split", new WordSpliter()).shuffleGrouping("count");		
		builder.setBolt("operate", new bolt("G:\\output")).fieldsGrouping("query", new Fields("word")).fieldsGrouping("split", new Fields("word"));
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("join-example", conf, builder.createTopology());

		Utils.sleep(2000);
		//    cluster.shutdown();
	}
}
