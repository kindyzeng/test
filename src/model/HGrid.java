package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.SpatialReference;


public class HGrid implements Serializable {

	public HashTable hashTLeft ;
	public HashTable hashTRight;
	public List<HResultEntity>resultSet;
	public HGrid(SpatialReference sr) {
		super();
		hashTRight = new HashTable(sr);
		hashTLeft = new HashTable(sr);
		resultSet = new ArrayList<HResultEntity>();
	}
	
}
