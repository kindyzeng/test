package com.grid;


import java.io.Serializable;

public class Grid implements Serializable{
	private static final long serialVersionUID = 4302512588111264086l;
     public Grid(int Maxnum) {
		super();
		this.BucKet = new BucKet(Maxnum);
	}
    private BucKet BucKet;
   
	public BucKet getBucKet() {
		return BucKet;
	}
	public void setBucKet(BucKet bucKet) {
		BucKet = bucKet;
	}
	public int Getdx()
	{
		return this.BucKet.getIdx();
	}
	public BucKet insert(Entity entity)
	{
		return BucKet.insert(entity);
	}
	public boolean delete(String id)
	{
		return BucKet.Delete(id);
	}

}
