package index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esri.core.geometry.Point;


//
public class test2 {	



	public static void main(String[] args) throws Exception {

		//		Map<String, Point>map;
		//		map = new HashMap<String, Point>();
		//		
		//         Point aPoint =new Point(1, 2); 
		//         Point bPoint = aPoint;
		////         bPoint.setX(8);
		//		System.out.print(aPoint.getX());
		//		map.put("1", new Point(1, 2));
		//		map.replace("1", new Point(3, 2));
		//		map.remove("1");

		//		List<Point>lst = new ArrayList<Point>();
		//		lst.add(new Point(1, 2));
		//		lst.add(new Point(3, 8));
		//		lst.add(new Point(7, 2));
		//		lst.set(1, new Point(5, 5));
		//		Point point = lst.get(1);
		//		point =new Point(3,8);
		//		point.setX(8);
		//		System.out.print(lst.get(1).getX());
		//		
		List<String>Lst = new ArrayList<>();
		Lst =ADDList(Lst,0);
		System.out.print("a");

	}
	public  static List<String> ADDList(List<String>lst,int k)
	{
		if(k==4)
		{
			return lst;
		}
		else 
		{
			lst.add("a");
			return ADDList(lst,k+1);
		}

	}

}
