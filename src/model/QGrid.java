package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.QuadTree;


public class QGrid implements Serializable {
	/**
	 * 
	 */
	public QuadTree qtreeLeft ;
	public QuadTree qtreeRight;
	public String Grid;
	public List<QResultEntity>resultSet;
	public QGrid(String gridid) {
		super();
		String gridids[]=gridid.split(",");
		int r=Integer.parseInt(gridids[0]);
		int c=Integer.parseInt(gridids[1]);
		qtreeLeft =new QuadTree(new Envelope2D(c*238,r*234, (c+1)*238, (r+1)*234), 8);
		qtreeRight = new QuadTree(new Envelope2D(c*238,r*234, (c+1)*238, (r+1)*234), 8);
		Grid=gridid;
		resultSet = new ArrayList<QResultEntity>();
	}
	
}
