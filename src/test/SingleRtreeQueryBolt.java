package test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;


//23800*23400

public  class SingleRtreeQueryBolt extends BaseBasicBolt  {  
	private  RTree<String, Point> tree ;
	private String Path;
	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		File file = new File(Path);
		tree= RTree.create().maxChildren(5).create();
		java.util.List<String> lines = new ArrayList<>();
		try {
			lines = FileUtils.readLines(file,"UTF-8");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long begin = System.currentTimeMillis();
		int k=0;
		int m=0;
		for (String line : lines) {
			if(line.split("\t")[0].equals("newpoint"))
			{
				m++;
				tree = tree.add(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
			}

		}
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {


		if(input.getSourceComponent().equalsIgnoreCase("QuerySpout"))   
		{
			String Query =  input.getString(0);
			String queryID = Query.split(",")[0];
			Double Xmin = Double.valueOf(Query.split(",")[1]);
			Double Ymin = Double.valueOf(Query.split(",")[2]);
			Double Xmax =Double.valueOf(Query.split(",")[3]);
			Double Ymax = Double.valueOf(Query.split(",")[4]);
			Iterable<Entry<String, Point>> it = tree.search(Geometries.rectangle(Xmin,Ymin,Xmax,Ymax))
					.toBlocking().toIterable();
			Iterator<Entry<String, Point>> iter = it.iterator();
			String result="";
			if(iter.hasNext())
			{
				result+= iter.next().value();	
			}
			while(iter.hasNext())
			{   
				result=result+","+iter.next().value();	
			}
			if(!result.equals(""))
			{
				collector.emit(new Values(queryID,result));   
			}
			else
			{
				collector.emit(new Values(queryID,"Nodate"));   
			}
		}

	}


	public SingleRtreeQueryBolt(String path) {
		super();
		this.Path = path;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("QueryID","Result"));
	} 
}