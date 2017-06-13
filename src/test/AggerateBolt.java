package test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import model.ResultEntity;
import model.RtreeGrid;
import rx.functions.Func2;


//23800*23400

public  class AggerateBolt extends BaseBasicBolt  {  
	private  HashMap<String, String>result ;
	@Override 
	public void prepare(Map conf,TopologyContext context)
	{

		result =new HashMap<String, String>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String QueryID = input.getString(0);
		String str = input.getString(1);
		String res = result.get(QueryID);
		
		if (res == null)
		{
			result.put(QueryID, str);
		}else {
			result.put(QueryID, res+","+str);
		}
	}

	public AggerateBolt() {
		super();
	}




	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(""));
	} 
}