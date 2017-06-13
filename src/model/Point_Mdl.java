package model;

import com.esri.core.geometry.Point;

public class Point_Mdl {

	private Integer _id;	
	private Point _point;
	private Point _oldpoint;
	////    private Date _date;
	//    private String operation;

//	private boolean isnew;

	//public Date get_date() {
	//		return _date;
	//	}
	//
	//public void set_date(Date _date) {
	//		this._date = _date;
	//	}

	public Point get_oldpoint() {
		return _oldpoint;
	}
	public Integer get_id() {
		return _id;
	}
	public void set_oldpoint(Point _oldpoint) {
		this._oldpoint = _oldpoint;
	}
	public void set_id(Integer _id) {
		this._id = _id;
	}
//	public boolean isIsnew() {
//		return isnew;
//	}
	public Point_Mdl(int _id, Point _point) {
		super();
		this._id = _id;
		this._point = _point;
//		this.isnew = isnew;
	}
//	public void setIsnew(boolean isnew) {
//		this.isnew = isnew;
//	}
	public Point get_point() {
		return _point;
	}
	public void set_point(Point _point) {
		this._point = _point;
	}


}
