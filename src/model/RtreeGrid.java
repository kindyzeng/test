package model;

import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Point;

public class RtreeGrid  {
	
	public RTree<String, Point> rtreeLeft ;
	public RTree<String, Point> rtreeRight;
	public String Grid;
	public List<ResultEntity>resultSet;
	public RtreeGrid() {
		super();
		rtreeLeft =RTree.star().maxChildren(5).create();
		rtreeRight = RTree.star().maxChildren(5).create();
		Grid="";
		resultSet = new ArrayList<>();
	}
	
}
