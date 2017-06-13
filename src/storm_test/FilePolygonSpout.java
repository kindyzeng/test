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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FilePolygonSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	String inputPath = "";
	String OutputPath = "";
	File f1 ;
	public FilePolygonSpout(String inputPath,String outputPath) {
		super();
		this.inputPath = inputPath;
		this.OutputPath = outputPath;

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		 this.f1 = new File(inputPath);
//		inputPath = "G://index//";
//		OutputPath = "G://output//";

	}

	@Override
	public void nextTuple() {

		if (f1.isDirectory()&&f1.listFiles().length>0) {  
			for (File f : f1.listFiles()) {
				try {
					java.util.List<String> lines = FileUtils.readLines(f,"UTF-8");
					for (String line : lines) {
						_collector.emit(new Values(line));
					}
				FileUtils.moveFile(f, new File(OutputPath + "/"+System.currentTimeMillis() + ".txt"));
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}  

	
	}

	@Override
	public void ack(Object id) {
		System.out.println("OK¡À");
	}

	@Override
	public void fail(Object id) {
		System.out.println("SB¡À");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}