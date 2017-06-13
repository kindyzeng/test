package test;

import java.util.Iterator;
import java.util.Map;

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

public  class DistributeSingleRtreeIndexBolt extends BaseBasicBolt  {  
	private  RTree<String, Point> tree ;
	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		tree= RTree.create().maxChildren(5).create();

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		if(input.getSourceComponent().equalsIgnoreCase("UpdateSpout"))
		{
			String line = input.getString(0);
//			String id = line.split("\t")[1];
			if(line.split("\t")[0].equals("newpoint"))
			{

				tree = tree.add(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
			}
			if(line.split("\t")[0].equals("point"))
			{

				tree = tree.delete(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
				tree = tree.add(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[4]), Double.parseDouble(line.split("\t")[5])));
			}
			if(line.split("\t")[0].equals("disappearpoint"))
			{		
				tree = tree.delete(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
			}		
		}
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


	public DistributeSingleRtreeIndexBolt() {
		super();
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("QueryID","Result"));
	} 
}