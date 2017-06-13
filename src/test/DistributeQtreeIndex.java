package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import model.QGrid;
import model.QResultEntity;
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
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;


//23800*23400

public  class DistributeQtreeIndex extends BaseBasicBolt  {  

	/**
	 * 
	 */
	private Map<String, QGrid> grids;
	private Map<Integer,Point> featureClass;
	private double joinDistance;
	private  SpatialReference sr;
	Envelope2D env2d ;


	@Override
	public void prepare(Map conf,TopologyContext context){

	    env2d = new Envelope2D();
		this.sr = SpatialReference.create(4326);
		this.featureClass=new HashMap<Integer,Point>();
		this.grids = new HashMap<String, QGrid>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition")||input.getSourceComponent().equalsIgnoreCase("RightUpdatePartition"))
		{
			String gridId = input.getString(0);
			QGrid grid  = grids.get(gridId);
			if(grid == null)
			{
				grid =new QGrid(gridId);

			}
			String point = input.getString(1);
			String[] multipoint = point.split(",");
			String Method =multipoint[0];
			String pid =multipoint[1];
			double x =Double.parseDouble(multipoint[2]);
			double y =Double.parseDouble(multipoint[3]);
			int element=Integer.parseInt(pid);
			Geometry geo=new Point(x,y);
			featureClass.put(element, (Point)geo);

			if(input.getSourceComponent().equalsIgnoreCase("LeftUpdatePartition"))
			{

				if(Method.equals("DELETE"))
				{
					QuadTreeIterator quadTreeIteratorLeft=grid.qtreeLeft.getIterator(geo, 0);
					int elementhandle=quadTreeIteratorLeft.next();
					if(elementhandle>=0){
						grid.qtreeLeft.removeElement(elementhandle);
						featureClass.remove(element);
					}
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).leftId==Integer.parseInt(pid))
						{
							grid.resultSet.remove(i);
							i--;
						}
					}
				}
				if(Method.equals("ADD"))
				{
					
					geo.queryEnvelope2D(env2d);
					grid.qtreeLeft.insert(element, env2d);

					QuadTreeIterator quadTreeIteratorRight=grid.qtreeRight.getIterator(geo, joinDistance);
					int elementHandle=-1;
					elementHandle=quadTreeIteratorRight.next();
					while(elementHandle>=0){
						int rightid=grid.qtreeRight.getElement(elementHandle);
						QResultEntity resultEntity = new QResultEntity();
						resultEntity.leftId = Integer.parseInt(pid);
						resultEntity.leftPoint = new Point(x,y) ;
						resultEntity.rightid = rightid;
						resultEntity.rightPoint = featureClass.get(new Integer(rightid));
						grid.resultSet.add(resultEntity);
						elementHandle=quadTreeIteratorRight.next();
					}
					grids.put(gridId, grid);
				}
				if(Method.equals("UPDATE"))
				{
					QuadTreeIterator quadTreeIteratorLeft=grid.qtreeLeft.getIterator(geo, 0);					
					int elementhandle=quadTreeIteratorLeft.next();
					if(elementhandle>=0){
						grid.qtreeLeft.removeElement(elementhandle);
					}
					double newx = Double.parseDouble(multipoint[4]);
					double newy	= Double.parseDouble(multipoint[5]);
					Geometry newgeo=new Point(newx,newy);
					featureClass.put(element, (Point)newgeo);
				
					newgeo.queryEnvelope2D(env2d);
					grid.qtreeLeft.insert(element, env2d);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).leftId==Integer.parseInt(pid))
						{
							grid.resultSet.remove(grid.resultSet.get(i));
							i--;
						}
					}

					QuadTreeIterator quadTreeIteratorRight=grid.qtreeRight.getIterator(newgeo, joinDistance);
					int elementHandle=-1;
					elementHandle=quadTreeIteratorRight.next();
					while(elementHandle>=0){
						int rightid=grid.qtreeRight.getElement(elementHandle);
						QResultEntity resultEntity = new QResultEntity();
						resultEntity.leftId = Integer.parseInt(pid);
						resultEntity.leftPoint = new Point(x,y) ;
						resultEntity.rightid = rightid;
						resultEntity.rightPoint = featureClass.get(new Integer(rightid));
						grid.resultSet.add(resultEntity);
						elementHandle=quadTreeIteratorRight.next();
					}
				}
			}
			else {
				if(Method.equals("DELETE"))
				{
					QuadTreeIterator quadTreeIteratorRight=grid.qtreeRight.getIterator(geo, 0);
					int elementhandle=quadTreeIteratorRight.next();
					if(elementhandle>=0){
						grid.qtreeRight.removeElement(elementhandle);
						featureClass.remove(element);
					}
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).rightid==Integer.parseInt(pid))
						{
							grid.resultSet.remove(i);
							i--;
						}

					}
				}
				if(Method.equals("UPDATE"))
				{		
					
					QuadTreeIterator quadTreeIteratorRight=grid.qtreeRight.getIterator(geo, 0);					
					int elementhandle=quadTreeIteratorRight.next();
					if(elementhandle>=0){
						grid.qtreeRight.removeElement(elementhandle);
					}
					double newx = Double.parseDouble(multipoint[4]);
					double newy	= Double.parseDouble(multipoint[5]);
					Geometry newgeo=new Point(newx,newy);
					featureClass.put(element, (Point)newgeo);

					newgeo.queryEnvelope2D(env2d);
					grid.qtreeRight.insert(element, env2d);
					for(int i=0;i<grid.resultSet.size();i++)
					{
						if(grid.resultSet.get(i).rightid==Integer.parseInt(pid));
						{
							grid.resultSet.remove(grid.resultSet.get(i));
							i--;
						}
					}
					QuadTreeIterator quadTreeIteratorLeft=grid.qtreeLeft.getIterator(newgeo, joinDistance);
					int elementHandle=-1;
					elementHandle=quadTreeIteratorLeft.next();
					while(elementHandle>=0){
						int leftid=grid.qtreeLeft.getElement(elementHandle);
						QResultEntity resultEntity = new QResultEntity();
						resultEntity.rightid = Integer.parseInt(pid);
						resultEntity.rightPoint = new Point(x,y) ;
						resultEntity.leftId = leftid;
						resultEntity.leftPoint = featureClass.get(new Integer(leftid));
						grid.resultSet.add(resultEntity);
						elementHandle=quadTreeIteratorLeft.next();
					}

				}
				if(Method.equals("ADD"))
				{
					geo.queryEnvelope2D(env2d);
					grid.qtreeRight.insert(element, env2d);

					QuadTreeIterator quadTreeIteratorLeft=grid.qtreeLeft.getIterator(geo, joinDistance);
					int elementHandle=-1;
					elementHandle=quadTreeIteratorLeft.next();
					while(elementHandle>=0){
						int leftid=grid.qtreeLeft.getElement(elementHandle);
						QResultEntity resultEntity = new QResultEntity();
						resultEntity.rightid = Integer.parseInt(pid);
						resultEntity.rightPoint = new Point(x,y) ;
						resultEntity.leftId = leftid;
						resultEntity.leftPoint = featureClass.get(new Integer(leftid));
						grid.resultSet.add(resultEntity);
						elementHandle=quadTreeIteratorLeft.next();
					}

					grids.put(gridId, grid);
				}
			}
		}
		else
		 {
			QGrid grid  = grids.get(input.getString(0));
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










	public DistributeQtreeIndex(double joinDistance) {
		super();
		this.joinDistance = joinDistance;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//		declarer.declare(new Fields("GridIndex","Point"));
	} 
}