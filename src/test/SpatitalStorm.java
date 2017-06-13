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

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SpatitalStorm {
	private static String abc;
	public static class UpdtaePartition extends BaseBasicBolt {
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			//锟斤拷锟斤拷位4km*锟斤拷641/160锟斤拷锟杰癸拷锟斤拷216*160锟斤拷 
			String line = input.getString(0);
			String[] points = line.split(",");
			String id = points[0];
			String oldx = points[1];
			String oldy = points[2];
			String newx = points[3];
			String newy = points[4];
			String oldrow = String.valueOf((int)Double.parseDouble(oldy)/4);
			String oldcol = String.valueOf((int)(Double.parseDouble(oldx)/641*160));
			String newrow = String.valueOf((int)Double.parseDouble(newy)/4);
			String newcol = String.valueOf((int)(Double.parseDouble(newx)/641*160));
			collector.emit(new Values(oldrow+","+oldcol,"UPDATE,"+id+","+newrow+","+newcol));
			if(!(oldrow.equals(newrow)&&oldcol.equals(newcol)))
			{
				collector.emit(new Values(oldrow+","+oldcol,"DELETE,"+id+","+newrow+","+newcol));
			}


		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("GridIndex","Point"));
		} 
	}

	public static class QueryPartition extends BaseBasicBolt {

		private  SpatialReference sr;
		public static Logger LOG ;
		@Override 
		public void prepare(Map conf,TopologyContext context)
		{
			sr = SpatialReference.create(4326);
			LOG = LoggerFactory.getLogger(QueryPartition.class);
		}
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String range = input.getString(0);
			if(range.split(",").length<4)
			{
				return;
			}
			if(range!="")
			{

				String[] points = range.split(",");		
				double Xmin = Double.parseDouble(points[0]);
				double Ymin = Double.parseDouble(points[1]);
				double Xmax = Double.parseDouble(points[2]);
				double Ymax = Double.parseDouble(points[3]);
				Envelope envelopeQuery  = new Envelope(Xmin, Ymin, Xmax, Ymax);
				int rowMax = (int)(Ymax/4);
				int rowMin = (int)(Ymin/4);
				int colMax = (int)(Xmax/641*160);
				int colMin = (int)(Xmin/641*160);
				UUID uuid = UUID.randomUUID();
				String id=uuid.toString()+","+(rowMax-rowMin+1)*(colMax-colMin+1);
				if(rowMax ==rowMin &&colMax==colMin)
				{
					collector.emit(new Values(rowMax+","+rowMax,Xmin+","+Ymin+","+Xmax+","+Ymax,range,id));
				}
				int k = 0;
				for(int i=rowMin;i<=rowMax;i++)
				{
					for(int j = colMin;j<=colMax;j++)
					{	    		
						Envelope envelopeIndex  = new Envelope(j*641/160,i*4, (j+1)*641/160, (i+1)*4 );
						if(GeometryEngine.overlaps(envelopeIndex, envelopeQuery, sr))
						{
							k++;
							collector.emit(new Values(i+","+j,Xmin+","+Ymin+","+Xmax+","+Ymax,range,id));
						}
						if(GeometryEngine.within(envelopeIndex, envelopeQuery, sr))
						{
							k++;
							collector.emit(new Values(i+","+j,"all",range,id));
						}
					}
				}
				LOG.warn("fuckzzzzzyy"+String.valueOf((rowMax-rowMin+1)*(colMax-colMin+1)+","+k));
			}

		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("GridIndex","Point","Range","StreamInfo"));
		} 
	}


	public static class SpatitalIndex extends BaseBasicBolt {
		Map<String, String> points = new HashMap<String, String>();
		private  SpatialReference sr;
		String _path;
		public static Logger LOG ;
		public SpatitalIndex() {
			super();
		}
		@Override 
		public void prepare(Map conf,TopologyContext context)
		{
			sr = SpatialReference.create(4326);
			LOG = LoggerFactory.getLogger(SpatitalIndex.class);
		}
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {

			if(tuple.getSourceComponent().equalsIgnoreCase("UpdatePartition"))
			{
				String point = tuple.getString(1);
				String[] multipoint = point.split(",");
				String Method =multipoint[0];
				String  ID = multipoint[1];
				String  X = multipoint[2];
				String  Y = multipoint[3];
				if (Method.endsWith("UPDATE"))
				{
					String XY = points.get(ID);
					if (XY == null)
					{
						points.put(ID, X+","+Y);
					}
					else
					{
						points.remove(ID);
						points.put(ID, X+","+Y);
					}
				}	
				if (Method == "DELETE")
				{
					String XY = points.get(ID);
					if (XY != null)
					{
						points.remove(ID);
					}
				}	

			}
			else {
				String point = tuple.getString(1);

				String result ="";
				if(!points.isEmpty())
				{
					if(point.startsWith("all"))
					{

						Iterator iter = points.entrySet().iterator();
						while (iter.hasNext()) {
							Map.Entry entry = (Map.Entry) iter.next();
							Object key = entry.getKey();
							Object val = entry.getValue();
							result = result+","+key.toString();
						}

					}
					else {

						String[] rec = tuple.getString(1).split(",");
						Iterator iter = points.entrySet().iterator();
						Envelope envelope = new Envelope(Double.valueOf(rec[0]), Double.valueOf(rec[1]), Double.valueOf(rec[2]), Double.valueOf(rec[3]));
						while (iter.hasNext()) {
							Map.Entry entry = (Map.Entry) iter.next();
							Object key = entry.getKey();
							Object val = entry.getValue();			
							String xy[] = val.toString().split(",");
							Point ptr = new Point(Double.valueOf(xy[0]), Double.valueOf(xy[1]));
							if(GeometryEngine.contains(ptr, envelope, sr))
							{
								result = result+","+key.toString();
							}
						}  	          
					}
				}

				if(result.endsWith(""))
				{
					result = "null";
				}
				collector.emit(new Values(result,tuple.getString(2),tuple.getString(3))); 

			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("Result","Range","StreamID"));
		}
	}


	public static class Output extends BaseBasicBolt {
		public static Logger LOG;
		String _path;
		Map<String, String> points = new HashMap<String, String>();
		@Override 
		public void prepare(Map conf,TopologyContext context)
		{
			LOG = LoggerFactory.getLogger(Output.class);
		}
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			//锟斤拷时锟斤拷锟斤拷慕锟斤拷
			String tempResult = "";
			//锟揭碉拷锟侥斤拷锟�			
			String result = input.getString(0);
			//锟斤拷围
			String range = input.getString(1);
			String keynum = input.getString(2);
			String id = keynum.split(",")[0];		
			//锟斤拷锟斤拷
			int expectum  = Integer.parseInt(keynum.split(",")[1]);
			LOG.warn(String.valueOf(expectum));
			
			if(expectum!=1)
			{
				String value =points.get(id);
				if(value == null)
				{
					points.put(id, value+"#1");
					return ;
				}
				else {
					int a =Integer.valueOf(value.split("#")[1])+1;
					//锟叫讹拷锟角凤拷锟角凤拷锟斤拷锟斤拷锟斤拷锟饺拷酆锟�
					if(a==expectum)
					{
						tempResult = value.split("#")[0]+","+result;
						//锟斤拷锟斤拷全锟桔合碉拷锟斤拷要锟斤拷时删锟斤拷锟斤拷锟斤拷诖锟斤拷锟斤拷
						points.remove(id);
					}
					else {					
						points.put(id, value.split("#")[0]+","+result+"#"+a);
						return;
					}
				}

			}
			else {
				tempResult = result;
			}

			String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); 
			FileWriter fw = null; 
			String arr[] = tempResult.split(",");
			//锟斤拷锟节硷拷锟斤拷锟叫讹拷锟劫革拷nodata
			int nodataNum=0;
			//锟斤拷锟节硷拷锟斤拷锟斤拷锟秸斤拷锟�
			String finalResult="";
			for (String string : arr) {
				if(!string.endsWith("null"))
				{
					finalResult=finalResult+","+string;	
				}		
			}
			if(finalResult.endsWith(""))
			{
				finalResult = "no data";
			}
			try {	
				fw = new FileWriter(_path,true);
				fw.write("result is "+finalResult+" time is "+time+" range is "+range+"\n"); 
				fw.close();   
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  

		}
		public Output(String _path) {
			super();
			this._path = _path;
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		} 
	}
	public static void main(String[] args) throws Exception {

		System.out.print("fuck");
		String updateTopic = "update";
		String queryTopic = "query";
		String zkRoot ="/storm";	
		String spoutUpdateId = "update";
		String spoutQueryId = "query";
		BrokerHosts brokerHosts = new ZkHosts("202.121.180.101:2181,202.121.180.102:2181,202.121.180.103:2181"); 
		SpoutConfig kafkaUpdateConfig = new SpoutConfig(brokerHosts, updateTopic, zkRoot, spoutUpdateId);
		SpoutConfig kafkaQueryConfig  =new SpoutConfig(brokerHosts, queryTopic, zkRoot, spoutQueryId);
		kafkaUpdateConfig.forceFromStart = true;
		kafkaQueryConfig.forceFromStart =true;
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(5);
		conf.put(Config.NIMBUS_HOST, "202.121.180.100");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("202.121.180.101","202.121.180.102","202.121.180.103"));
		kafkaUpdateConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaQueryConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("UpdateSpout", new KafkaSpout(kafkaUpdateConfig), 6);
		builder.setSpout("QuerySpout", new KafkaSpout(kafkaQueryConfig), 1);
		builder.setBolt("UpdatePartition", new UpdtaePartition(), 6).shuffleGrouping("UpdateSpout");	
		builder.setBolt("QueryPartition", new QueryPartition(), 2).shuffleGrouping("QuerySpout");
		builder.setBolt("SpatitalIndex", new SpatitalIndex(),12).fieldsGrouping("UpdatePartition", new Fields("GridIndex")).fieldsGrouping("QueryPartition", new Fields("GridIndex"));
		//		builder.setBolt("SpatitalIndex", new SpatitalIndex(),1).fieldsGrouping("QueryPartition", new Fields("GridIndex"));
		builder.setBolt("Output", new Output(args[0]),6).fieldsGrouping("SpatitalIndex", new Fields("StreamID"));
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
