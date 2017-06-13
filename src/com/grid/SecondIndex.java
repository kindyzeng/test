package com.grid;


public class SecondIndex {

	private String id
	;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setIdx(int idx) {
		this.idx = idx;
	}
	private BucKet bucKet;
	private int idx;
	private Grid grid;

	public BucKet getBucKet() {
		return bucKet;
	}
	public int getIdx() {
		return idx;
	}
	public void setBucKet(BucKet bucKet) {
		this.bucKet = bucKet;
	}


	public SecondIndex(String id, BucKet bucKet, int idx, Grid grid) {
		super();
		this.id = id;
		this.bucKet = bucKet;
		this.idx = idx;
		this.grid = grid;
	}
	public Grid getGrid() {
		return grid;
	}
	public void setGrid(Grid grid) {
		this.grid = grid;
	}

}
