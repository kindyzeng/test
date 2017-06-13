package com.grid;

import java.io.Serializable;
import java.util.List;


public class Entity implements Serializable{
	
	public Entity(String iD, double x, double y) {
		super();
		ID = iD;
		this.x = x;
		this.y = y;
	}
	private String ID;
	private double x;
	private double y;
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	
	//Xmin,Ymin,Xmax,Ymax˳�����ж��Լ�����
	public Boolean isContain(List<Double>Rec)
	{
		if(this.x>=Rec.get(0)&&this.x<=Rec.get(3)&&this.y>=Rec.get(2)&&this.y<=Rec.get(4))
		{
			return true;
		}
		else {
			{
				return false;
			}
		}
	}

}
