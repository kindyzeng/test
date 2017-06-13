package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

public class HashTable {
    private  Map<String, Point>hashTable;
    private  SpatialReference sr ;
	public HashTable(SpatialReference _sr) {
		super();
		hashTable = new HashMap<String, Point>();
		this.sr = _sr;
		
	}
	
	public int getMapSize()
	{
		return hashTable.size();
	}
   public void Insert(String id,Point point)
   {
	   hashTable.put(id, point);	   
   }
   public void delete(String Id)
   {
	   hashTable.remove(Id);
   }
   public void update(String Id,Point point)
   {
		Point point1 = hashTable.get(Id);
		if (point1 == null)
		{
			hashTable.put(Id, point);
		}
		else
		{
			hashTable.remove(Id);
			hashTable.put(Id, point);
		}
   }
   
   public HashMap<String, Point> SerchRec(Envelope envelope)
   {
	   HashMap<String, Point>pLst = new HashMap<>();
	   Iterator iter = hashTable.entrySet().iterator();
	   while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String)entry.getKey();
			Point val = (Point)entry.getValue();			
			
			if(GeometryEngine.contains(val, envelope, sr))
			{
				pLst.put(key, val);
			}
		}
	   return pLst;
   }
   
   public HashMap<String, Point> SerchCircle(Point Center,double r)
   {
	   HashMap<String, Point>pLst = new HashMap<>();
	   Iterator iter = hashTable.entrySet().iterator();
	   while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String)entry.getKey();
			Point val = (Point)entry.getValue();		
			if(Distance(val,Center)<r)
			{
				pLst.put(key, val);
			}	   
		}
	   return pLst;
   }
   
   private double Distance(Point pt1,Point pt2)
   {
	   
	   return Math.sqrt(Math.pow(pt1.getX()-pt2.getX(),2)+Math.pow(pt1.getY()-pt2.getY(),2));
   }
}
