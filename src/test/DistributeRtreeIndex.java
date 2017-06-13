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
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import model.ResultEntity;
import model.RtreeGrid;
import rx.functions.Func2;


//23800*23400

public  class DistributeRtreeIndex extends BaseBasicBolt  {  
	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		grids = new HashMap<String, RtreeGrid>();
		//		logout ="";
		//		try {
		//			logfwriter = new FileWriter(logOutPutPath,true);
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
		//		logbWriter = new BufferedWriter(logfwriter);
		sr = SpatialReference.create(4326);
		distance = new Func2<Point, Point, Double>() {

			@Override
			public Double call(Point arg0, Point arg1) {	
				// TODO Auto-generated method stub
				return Math.sqrt(Math.pow(arg0.x()-arg1.x(),2)+Math.pow(arg0.y()-arg1.y(),2));
			}
		};
	}
	/**
	 * 
	 */

	private Map<String, RtreeGrid>grids ;
	private double joinDistance;
	private String outputpath;
	private String logOutPutPath;
	private  SpatialReference sr;
	//	private String logout;
	//	private FileWriter logfwriter;
	//	private BufferedWriter logbWriter;
	private Func2<? super Point, ? super Point, Double> distance;
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//		logout ="";
		if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition")||input.getSourceComponent().equalsIgnoreCase("RightUpdatePartition"))
		{


			String gridId = input.getString(0);
			RtreeGrid grid  = grids.get(gridId);
			if(grid == null)
			{
				grid =new RtreeGrid();
				grid.Grid = gridId;
			}



			String point = input.getString(1);
			String[] multipoint = point.split(",");
			String Method =multipoint[0];
			String pid =multipoint[1];

			double x =Double.parseDouble(multipoint[2]);
			double y =Double.parseDouble(multipoint[3]);
			if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition"))
			{
				if(Method.equals("DELETE"))
				{


					grid.rtreeLeft = grid.rtreeLeft.delete(pid, Geometries.point(x,y));
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
					grid.rtreeLeft = grid.rtreeLeft.add(pid, Geometries.point(x, y));

					Iterable<Entry<String, Point>> it = grid.rtreeRight.search(Geometries.point(x, y), joinDistance, distance)
							.toBlocking().toIterable();
					Iterator<Entry<String, Point>> iter = it.iterator();
					while(iter.hasNext())
					{
						Entry<String, Point> a = iter.next();			
						ResultEntity resultEntity = new ResultEntity();
						resultEntity.leftId = pid;
						resultEntity.leftPoint = Geometries.point(x, y) ;
						resultEntity.rightid = a.value();
						resultEntity.rightPoint = a.geometry();
						grid.resultSet.add(resultEntity);
					}
					grids.put(gridId, grid);
				}
				if (Method.equals("UPDATE"))
				{
					grid.rtreeLeft = grid.rtreeLeft.delete(pid, Geometries.point(x, y));					
					double newx = Double.parseDouble(multipoint[4]);
					double newy	= Double.parseDouble(multipoint[5]);
					grid.rtreeLeft = grid.rtreeLeft.add(pid, Geometries.point(newx, newy));
					for(int i=0;i<grid.resultSet.size();i++)
					{

						if(grid.resultSet.get(i).leftId.equals(pid))
						{
							grid.resultSet.remove(grid.resultSet.get(i));
							i--;
						}

					}
					Iterable<Entry<String, Point>> it = grid.rtreeRight.search(Geometries.point(newx, newy), joinDistance, distance)
							.toBlocking().toIterable();
					Iterator<Entry<String, Point>> iter = it.iterator();


					while(iter.hasNext())
					{
						Entry<String, Point> a = iter.next();			
						ResultEntity resultEntity = new ResultEntity();
						resultEntity.leftId = pid;
						resultEntity.leftPoint = Geometries.point(newx, newy) ;
						resultEntity.rightid = a.value();
						resultEntity.rightPoint = a.geometry();
						grid.resultSet.add(resultEntity);
					}
				}

			}
			else {
				if(Method.equals("DELETE"))
				{		
					grid.rtreeRight = grid.rtreeRight.delete(pid, Geometries.point(x, y));
					for(int i=0;i<grid.resultSet.size();i++)
					{

						if(grid.resultSet.get(i).rightid.equals(pid))
						{
							grid.resultSet.remove(i);
							i--;
						}

					}
				}
				if (Method.equals("UPDATE"))
				{
					grid.rtreeRight = grid.rtreeRight.delete(pid, Geometries.point(x, y));					
					double newx = Double.parseDouble(multipoint[4]);
					double newy	= Double.parseDouble(multipoint[5]);
					grid.rtreeRight = grid.rtreeRight.add(pid, Geometries.point(newx, newy));
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
					Iterable<Entry<String, Point>> it = grid.rtreeLeft.search(Geometries.point(newx, newy), joinDistance, distance)
							.toBlocking().toIterable();
					Iterator<Entry<String, Point>> iter = it.iterator();
					while(iter.hasNext())
					{
						Entry<String, Point> a = iter.next();			
						ResultEntity resultEntity = new ResultEntity();
						resultEntity.rightid= pid;
						resultEntity.rightPoint = Geometries.point(newx, newy) ;
						resultEntity.leftId = a.value();
						resultEntity.leftPoint = a.geometry();
						grid.resultSet.add(resultEntity);
					}
				}
				if(Method.equals("ADD"))
				{
					grid.rtreeRight = grid.rtreeRight.add(pid, Geometries.point(x, y));
					Iterable<Entry<String, Point>> it = grid.rtreeLeft.search(Geometries.point(x, y), joinDistance, distance)
							.toBlocking().toIterable();
					Iterator<Entry<String, Point>> iter = it.iterator();
					while(iter.hasNext())
					{
						Entry<String, Point> a = iter.next();			
						ResultEntity resultEntity = new ResultEntity();
						resultEntity.rightid = pid;
						resultEntity.rightPoint = Geometries.point(x, y) ;
						resultEntity.leftId = a.value();
						resultEntity.leftPoint = a.geometry();
						grid.resultSet.add(resultEntity);
					}
					grids.put(gridId, grid);
				}

			}
		}
		if(input.getSourceComponent().equalsIgnoreCase("QueryPartition"))   
		{
			FileWriter writer;
			try {
				writer = new FileWriter(outputpath,true);
				BufferedWriter bw = new BufferedWriter(writer);
				RtreeGrid grid  = grids.get(input.getString(0)); 
				StringBuffer sb= new StringBuffer("");
				String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); 
				sb.append("result for query "+input.getString(2)+" at time"+time);
				if(grid!=null)
				{
					if(input.getString(1).split(",")[0].equals("all"))
					{

						if(grid.resultSet.size()>0)
						{
							for(int i=0;i<grid.resultSet.size();i++)
							{
								sb.append(grid.resultSet.get(i).leftId+","+grid.resultSet.get(i).rightid+"\t");
							}

						}
						else{

							sb.append("nodata");

						}

					}
					else
					{
						String[] rec = input.getString(1).split(",");
						Envelope envelope = new Envelope(Double.valueOf(rec[0]), Double.valueOf(rec[1]), Double.valueOf(rec[2]), Double.valueOf(rec[3]));
						if(grid.resultSet.size()!=0)
						{
							for(int i=0;i<grid.resultSet.size();i++)
							{	
								com.esri.core.geometry.Point ptl = new com.esri.core.geometry.Point(Double.valueOf(grid.resultSet.get(i).leftPoint.x()), Double.valueOf(grid.resultSet.get(i).leftPoint.y()));
								com.esri.core.geometry.Point ptr = new com.esri.core.geometry.Point(Double.valueOf(grid.resultSet.get(i).rightPoint.x()), Double.valueOf(grid.resultSet.get(i).rightPoint.y()));
								if(GeometryEngine.contains(ptl, envelope, sr)&&GeometryEngine.contains(ptr, envelope, sr))
								{

									sb.append(grid.resultSet.get(i).leftId+","+grid.resultSet.get(i).rightid+"\t");
								}	
							}
						}
						else
						{

							sb.append("nodata");
						}
					}

				}
				else
				{
					sb.append("nodata");
				}

				sb.append("\n");
				bw.write(sb.toString());
				bw.close();
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		//		try {
		//			logout = null;
		//			if(!logout.equals(""))
		//			{
		//				logout += "end!!!\n";
		//				logbWriter.write(logout);
		//			}
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}




	public DistributeRtreeIndex(double joinDistance,String path,String logOutPath) {
		super();
		this.joinDistance = joinDistance;
		this.outputpath = path;
		this.logOutPutPath = logOutPath;
	}




	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GridIndex","Point"));
	} 
}