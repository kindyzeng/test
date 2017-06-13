package test;

import java.util.HashMap;
import java.util.Map;

import com.esri.core.geometry.SpatialReference;
import com.github.davidmoten.rtree.geometry.Point;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import model.RtreeGrid;
import rx.functions.Func2;


//23800*23400

public  class testSpalIndex extends BaseBasicBolt  {  
	private double joinDistance;
	private String outputpath;
	private  SpatialReference sr;
	private Map<String, RtreeGrid> grids = new HashMap<String, RtreeGrid>();
		private Func2<? super Point, ? super Point, Double> distance;
	private Point point;
	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		sr = SpatialReference.create(4326);
		this.sr = SpatialReference.create(4326);
	}
	/**
	 * 
	 */

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.print("fuck!!!!!!!!"+input.getSourceComponent().toString());
		if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition")||input.getSourceComponent().equalsIgnoreCase("RightUpdatePartition"))
		{
			collector.emit(new Values(input.getString(0),input.getString(1))); 
		}
	}




	public testSpalIndex(double joinDistance,String path) {
		super();
		this.joinDistance = joinDistance;
		this.outputpath = path;


	}




	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GridIndex","Point"));
	} 
}