package test;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class QueryPartition extends BaseBasicBolt {

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
		if(range.split(",").length<5)
		{
			return;
		}
		if(range!="")
		{

			String[] points = range.split(",");	
			String ID = points[0];
			double Xmin = Double.parseDouble(points[1]);
			double Ymin = Double.parseDouble(points[2]);
			double Xmax = Double.parseDouble(points[3]);
			double Ymax = Double.parseDouble(points[4]);
			Envelope envelopeQuery  = new Envelope(Xmin, Ymin, Xmax, Ymax);
			int rowMax = (int)(Ymax/234);
			int rowMin = (int)(Ymin/234);
			int colMax = (int)(Xmax/238);
			int colMin = (int)(Xmin/238);
			//每一次查询都产生一个GUID，用用于识别流是哪里来的
//			UUID uuid = UUID.randomUUID();
//			String id=uuid.toString()+","+(rowMax-rowMin+1)*(colMax-colMin+1);
			if(rowMax ==rowMin &&colMax==colMin)
			{
				collector.emit(new Values(rowMax+","+rowMax,Xmin+","+Ymin+","+Xmax+","+Ymax,ID));
			}
			for(int i=rowMin;i<=rowMax;i++)
			{
				for(int j = colMin;j<=colMax;j++)
				{	    		
					Envelope envelopeIndex  = new Envelope(j*238,i*234, (j+1)*238, (i+1)*234);
					if(GeometryEngine.overlaps(envelopeIndex, envelopeQuery, sr))
					{
//						k++;
						collector.emit(new Values(i+","+j,Xmin+","+Ymin+","+Xmax+","+Ymax,ID));
					}
					if(GeometryEngine.within(envelopeIndex, envelopeQuery, sr))
					{
//						k++;
						collector.emit(new Values(i+","+j,"all",ID));
					}
				}
			}
		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GridIndex","Range","QueryID"));
	} 
}
