package test;
import java.util.HashMap;
import java.util.Iterator;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
@SuppressWarnings("serial")
public class SpatitalIndex implements ReducerAggregator<String>{

	@Override
	public String init() {
		// TODO Auto-generated method stub
		return "";
	}

	@Override
	public  String reduce(String arg0,	TridentTuple arg1) {
		// TODO Auto-generated method stub
		String newpoint = arg1.getString(1);
		String[] pointGeometry = newpoint.split(",");
		String Method =pointGeometry[0];
		String  ID = pointGeometry[1];
		String  X = pointGeometry[2];
		String  Y = pointGeometry[3];
		
		String multipoint[] = arg0.split("#");
		HashMap<String,String>map= new HashMap<String, String>();
		
		for (String string : multipoint) {
			 String point[] = string.split(",");
		 map.put(point[0], point[1]+","+ point[2]);
			
		}
		if (Method == "UPDATE")
		{
			String XY = map.get(ID);
			if (XY == null)
			{
				map.put(ID, X+","+Y);
			}
			else
			{
				map.remove(ID);
				map.put(ID, X+","+Y);
			}
		}	
		if (Method == "DELETE")
		{
			String XY = map.get(ID);
			if (XY != null)
			{
				map.remove(ID);
			}
		}
        String  str="";
		Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
        	String key = iter.next().toString();
        	String val = map.get(key).toString();
        	str =str+ key+","+val+"#";
		}
		return str;
	}


	


	
   
}
