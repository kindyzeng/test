package index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import rx.functions.Func2;

import model.ResultEntity;
import model.RtreeGrid;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

public class TestRtree {
	private static Map<String, RtreeGrid>grids ;
	private static double joinDistance;
	private static String outputpath;
	private static String logOutPutPath;
	private  static SpatialReference sr;
	private static int x ;
	private static Func2<? super Point, ? super Point, Double> distance;
	static File f1 ;
	public static void main(String[] args) throws Exception {
		grids = new HashMap<String, RtreeGrid>();
		sr = SpatialReference.create(4326);
		distance = new Func2<Point, Point, Double>() {

			@Override
			public Double call(Point arg0, Point arg1) {	
				// TODO Auto-generated method stub
				return Math.sqrt(Math.pow(arg0.x()-arg1.x(),2)+Math.pow(arg0.y()-arg1.y(),2));
			}
		};
		joinDistance = Double.parseDouble(args[0]);	 
		x= 0;
		int y=0;
		f1 = new File(args[1]);
		if (f1.isDirectory()&&f1.listFiles().length>0) {  
			for (File f : f1.listFiles()) {
				try {
					java.util.List<String> lines = FileUtils.readLines(f,"UTF-8");
					for (String line : lines) {

						System.out.print(y+++"\n"); 			
						excute(line);
						if(x<y)
						{
							System.out.print(line+"\n"); 
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
		System.out.print(x); 
	}
	private static void excute(String str)
	{
		
		String gridId = str.split("#")[0];
		RtreeGrid grid  = grids.get(gridId);
		if(grid == null)
		{
			grid =new RtreeGrid();
			grid.Grid = gridId;
		}

		String point = str.split("#")[1];
		String[] multipoint = point.split(",");
		String Method =multipoint[0];
		String pid =multipoint[1];

		double x =Double.parseDouble(multipoint[2]);
		double y =Double.parseDouble(multipoint[3]);
		
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
				x++;
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
				x++;
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
			x++;
	}
}
