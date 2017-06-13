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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FilePointSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	String inputPath = "";
	FileReader readerright ;
	BufferedReader brright ;
	File file;
	String str ="";
	File[] files;
	int fileNum;
	public FilePointSpout(String inputPath) throws FileNotFoundException {
		super();
		this.inputPath = inputPath;
		fileNum = 0;

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

		files = new File(inputPath).listFiles();
//		file = files[3];
		if(files.length>0)
		{
			file = files[0];
			try {
				this.readerright = new FileReader(file);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			this.brright =new BufferedReader(readerright);
		}

	}

	@Override
	public void nextTuple() {

		try {
			if(brright!=null&&(str=brright.readLine())!= null)
			{
				_collector.emit(new Values(str));
			}
			else {
				
				if(fileNum+1<files.length)
				{
					fileNum++;
					file = files[fileNum];
					this.readerright = new FileReader(file);
					this.brright =new BufferedReader(readerright);
					str=brright.readLine();
					_collector.emit(new Values(str));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void ack(Object id) {
		System.out.println("OKÁË");
	}

	@Override
	public void fail(Object id) {
		System.out.println("SBÁË");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}