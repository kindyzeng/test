package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import model.HGrid;
import model.HResultEntity;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;


//23800*23400

public  class  DistributeHashTableIndex extends BaseBasicBolt  {  


	private Map<String, HGrid> grids;
	private double joinDistance;
	private  SpatialReference sr;


	@Override
	public void prepare(Map conf,TopologyContext context){

		this.sr = SpatialReference.create(4326);
		this.grids = new HashMap<String, HGrid>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition")||input.getSourceComponent().equalsIgnoreCase("RightUpdatePartition"))
		{
			String gridId = input.getString(0);
			HGrid grid  = grids.get(gridId);
			if(grid == null)
			{
				grid =new HGrid(sr);

			}
			String point = input.getString(1);
			String[] multipoint = point.split(",");
			String Method =multipoint[0];
			String pid =multipoint[1];
			double x =Double.parseDouble(multipoint[2]);
			double y =Double.parseDouble(multipoint[3]);
			Point ptr= new Point(x,y);
			if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition"))
			{

				if(Method.equals("DELETE"))
				{

					grid.hashTLeft.delete(pid);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).leftId.equals(pid))
						{
							grid.resultSet.remove(i);
							i--;
						}
					}
				}
				if(Method.equals("ADD"))
				{

					grid.hashTLeft.Insert(pid,ptr);

					HashMap<String, Point>ptlst=grid.hashTRight.SerchCircle(ptr, joinDistance);
					Iterator iter = ptlst.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String)entry.getKey();
						Point val = (Point)entry.getValue();			
						HResultEntity entity =new HResultEntity();
						entity.leftId = pid;
						entity.leftPoint = ptr;
						entity.rightid = key;
						entity.rightPoint = val;
						grid.resultSet.add(entity);
					}	
					grids.put(gridId, grid);
				}

				if(Method.equals("UPDATE"))
				{
					Point newgeo=new Point(Double.parseDouble(multipoint[4]),Double.parseDouble(multipoint[5]));
					grid.hashTLeft.update(pid,ptr);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.size() != 0)
						{
							if(grid.resultSet.get(i).rightid.equals(pid))
							{
								grid.resultSet.remove(grid.resultSet.get(i));
								i--;
							}
						}
					}
					HashMap<String, Point>ptlst=grid.hashTRight.SerchCircle(ptr, joinDistance);
					Iterator iter = ptlst.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String)entry.getKey();
						Point val = (Point)entry.getValue();			
						HResultEntity entity =new HResultEntity();
						entity.leftId = pid;
						entity.leftPoint = ptr;
						entity.rightid = key;
						entity.rightPoint = val;
						grid.resultSet.add(entity);
					}

				}
			}
			else {
				if(Method.equals("DELETE"))
				{
					grid.hashTLeft.delete(pid);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).rightid.equals(pid))
						{
							grid.resultSet.remove(i);
							i--;
						}
					}
				}

				if(Method.equals("UPDATE"))
				{		
					Point newgeo=new Point(Double.parseDouble(multipoint[4]),Double.parseDouble(multipoint[5]));
					grid.hashTRight.update(pid,ptr);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.size() != 0)
						{
							if(grid.resultSet.get(i).rightid.equals(pid))
							{
								grid.resultSet.remove(grid.resultSet.get(i));
								i--;
							}
						}
					}
					HashMap<String, Point>ptlst=grid.hashTLeft.SerchCircle(ptr, joinDistance);
					Iterator iter = ptlst.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String)entry.getKey();
						Point val = (Point)entry.getValue();			
						HResultEntity entity =new HResultEntity();
						entity.rightid = pid;
						entity.rightPoint = ptr;
						entity.leftId = key;
						entity.leftPoint = val;;
						grid.resultSet.add(entity);
					}

				}
				if(Method.equals("ADD"))
				{
					grid.hashTRight.Insert(pid,ptr);

					HashMap<String, Point>ptlst=grid.hashTLeft.SerchCircle(ptr, joinDistance);
					Iterator iter = ptlst.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String)entry.getKey();
						Point val = (Point)entry.getValue();			
						HResultEntity entity =new HResultEntity();
						entity.rightid = pid;
						entity.rightPoint = ptr;
						entity.leftId = key;
						entity.leftPoint = val;
						grid.resultSet.add(entity);
					}	
					grids.put(gridId, grid);
				}
			}
		}

		else {


			HGrid grid  = grids.get(input.getString(0));
			StringBuffer sb= new StringBuffer("");
			if(grid!=null)
			{
				if(input.getString(1).split(",")[0].equals("all"))
				{


					for(int i=0;i<=grid.resultSet.size();i++)
					{
						sb.append(grid.resultSet.get(i).leftId+","+grid.resultSet.get(i).rightid+"\n");
					}

				}
				else
				{
					String[] rec = input.getString(1).split(",");
					Envelope envelope = new Envelope(Double.valueOf(rec[0]), Double.valueOf(rec[1]), Double.valueOf(rec[2]), Double.valueOf(rec[3]));
					for(int i=0;i<=grid.resultSet.size();i++)
					{	
						com.esri.core.geometry.Point ptl = new com.esri.core.geometry.Point(Double.valueOf(grid.resultSet.get(i).leftPoint.getX()), Double.valueOf(grid.resultSet.get(i).leftPoint.getY()));
						com.esri.core.geometry.Point ptr = new com.esri.core.geometry.Point(Double.valueOf(grid.resultSet.get(i).rightPoint.getX()), Double.valueOf(grid.resultSet.get(i).rightPoint.getY()));
						if(GeometryEngine.contains(ptl, envelope, sr)&&GeometryEngine.contains(ptr, envelope, sr))
						{
							sb.append(grid.resultSet.get(i).leftId+","+grid.resultSet.get(i).rightid+"\n");
						}	
					}
				}

			}
			else
			{
				sb.append("nodata\n");
			}
			String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); 
			sb.append("result for query "+input.getString(2)+" at time"+time);
		}
	}



	public DistributeHashTableIndex(double joinDistance) {
		super();
		this.joinDistance = joinDistance;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//		declarer.declare(new Fields("GridIndex","Point"));
	} 
}