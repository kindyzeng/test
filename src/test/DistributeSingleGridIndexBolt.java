package test;

import java.util.Iterator;
import java.util.Map;

import com.grid.Entity;
import com.grid.NormalGridIndex;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class DistributeSingleGridIndexBolt extends BaseBasicBolt {
	NormalGridIndex gridIndex;

	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		gridIndex =new NormalGridIndex();

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		if(input.getSourceComponent().equalsIgnoreCase("UpdateSpout"))
		{
			String line = input.getString(1);
			if(line.split("\t")[0].equals("newpoint"))
			{

				Entity entity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				gridIndex.Insert(entity);
			}
			if(line.split("\t")[0].equals("point"))
			{

				if(line.split("\t")[1].equals("96263"))
				{
					System.out.print("fuck");
				}
				Entity oldentity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				Entity newentity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[4]),Double.parseDouble(line.split("\t")[5]));
				gridIndex.Update(oldentity, newentity);
			}
			if(line.split("\t")[0].equals("disappearpoint"))
			{
				Entity entity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				gridIndex.Delete(entity);

			}

		}
		if(input.getSourceComponent().equalsIgnoreCase("QuerySpout"))   
		{
			String line =  input.getString(0);
			String queryID = line.split(",")[0];
			Double Xmin = Double.valueOf(line.split(",")[1]);
			Double Ymin = Double.valueOf(line.split(",")[2]);
			Double Xmax =Double.valueOf(line.split(",")[3]);
			Double Ymax = Double.valueOf(line.split(",")[4]);			
			gridIndex.RangeQuery(Xmin, Ymin, Xmax, Ymax);
		}

	}


	public DistributeSingleGridIndexBolt() {
		super();
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("QueryID","Result"));
	} 
}
