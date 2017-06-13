package com.grid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class UGrid implements Serializable{
	private static final long serialVersionUID = 4302512588111264086l;
	public Map<String, Grid> getGrids() {
		return grids;
	}

	private Map<String, Grid>grids;

	private static int Maxnum =8;
	private static double gridWidth=238;
	private static double gridlongth =234;

	//遍历UGrid内所有的点
	public List<Entity> returnAll(){
		List<Entity> entityList = new ArrayList<Entity>();

		Grid grid;
		for (Map.Entry<String,Grid> entry:grids.entrySet()){
			grid = entry.getValue();
			entityList.addAll(grid.getBucKet().SearchByRangeAll());
		}
		return  entityList;
	}

	public List<Entity> RangeQuery(double Xmin,double Ymin,double Xmax,double Ymax)
	{
		List<Entity>entityLst = new ArrayList<Entity>();
		int minCol =(int) (Xmin/gridWidth);
		int maxCol =(int) (Xmax/gridWidth);
		int minRow =(int) (Ymin/gridlongth);
		int maxRow =(int) (Ymax/gridlongth);
		for(int i =minCol;i<=maxCol;i++)
		{
			for(int j= minRow;j<=maxRow;j++)
			{
				if(i>minCol&&i<maxCol&&j>minRow&&j<maxRow)
				{

					List<Entity>tempEntities = GetEntityByGrid(j,i,null);
					if(tempEntities!=null)
					{
						for(int k=0;k<tempEntities.size();k++)
						{
						   entityLst.add(tempEntities.get(k));
						}
					}
				}
				else {
					List<Double>Lst = new ArrayList<Double>();
					Lst.add(gridWidth*i);
					Lst.add(gridlongth*j);
					Lst.add(gridWidth*(i+1));
					Lst.add(gridlongth*(j+1));
					List<Entity>tempEntities = GetEntityByGrid(j,i,Lst);

					if(tempEntities!=null)
					{
						for(int k=0;k<tempEntities.size();k++)
						{
							entityLst.add(tempEntities.get(k));
						}

					}

				}
			}
		}
		return entityLst;

	}

	private List<Entity> GetEntityByGrid(int row ,int col,List<Double>rec)
	{

		Grid grid = grids.get(col+","+row);
		if(grid !=null)
		{
			if(rec!=null)
			{
				return grid.getBucKet().SearchByRangeAll();
			}
			else {
				return grid.getBucKet().SearchByRangebyCondition(rec);
			}
		}
		return null;
	}

	public void Insert(Entity entity)
	{
		String Gridid = ComputeGrid(entity);
		Grid grid = grids.get(Gridid);

		if(grid ==null)
		{
			grid =new Grid(Maxnum);
			grids.put(Gridid, grid);
		}
		//		if(Gridid.equals("11,4"))
		//		{
		//			System.out.print("");
		//		}

		SecondIndex secondIndex =new SecondIndex(entity.getID(), grid.insert(entity),grid.Getdx(),grid);
		SecondIndexList.getInstance().add(secondIndex);


	}

	public UGrid() {
		super();
		grids =new HashMap<String, Grid>();

	}

	public void Disappear(Entity entity)
	{
		if(SecondIndexList.getInstance().Search(entity.getID())!=null)
		{
			String Gridid = (int)((entity.getX())/gridWidth)+","+(int)(entity.getY()/gridlongth);
			Grid grid = grids.get(Gridid);
			grid.delete(entity.getID());
		}
	}

	public void Update(String id,Entity newEntity){
		SecondIndex index =SecondIndexList.getInstance().Search(id);
		if(index==null)
		{
			this.Insert(newEntity);
		}
		else
		{
			Entity oldEntity =  index.getBucKet().entrylst.get(index.getIdx());
			String newGrid = ComputeGrid(newEntity);
			String oldGrid = ComputeGrid(oldEntity);

			//localupdate
			if(newGrid.equals(oldGrid))
			{
				index.getBucKet().entrylst.get(index.getIdx()).setX(newEntity.getX());
				index.getBucKet().entrylst.get(index.getIdx()).setY(newEntity.getY());

			}
			//nonlocalupdate
			else {
				Grid _oldGrid = grids.get(oldGrid);
				Grid _newGrid = grids.get(newGrid);
				if(_newGrid ==null)
				{
					_newGrid = new Grid(Maxnum);
					grids.put(newGrid, _newGrid);
				}

				_oldGrid.delete(oldEntity.getID());


				SecondIndex secondIndex =new SecondIndex(newEntity.getID(), _newGrid.insert(newEntity),_newGrid.Getdx(),_newGrid);
				SecondIndexList.getInstance().add(secondIndex);

				//				if(index.getBucKet().entrylst.size()==0)
				//				{
				//					System.out.print("fuck");
				//
				//				}
				//				index.setIdx(_newGrid.Getdx());
			}
		}
	}

	public void Update(Entity oldEntity,Entity newEntity)
	{
		SecondIndex index =SecondIndexList.getInstance().Search(oldEntity.getID());
		if(index==null)
		{
			this.Insert(newEntity);
		}
		else
		{
			String newGrid = ComputeGrid(newEntity);
			String oldGrid = ComputeGrid(oldEntity);

			//localupdate
			if(newGrid.equals(oldGrid))
			{
				index.getBucKet().entrylst.get(index.getIdx()).setX(newEntity.getX());
				index.getBucKet().entrylst.get(index.getIdx()).setY(newEntity.getY());

			}	
			//nonlocalupdate
			else {
				Grid _oldGrid = grids.get(oldGrid);
				Grid _newGrid = grids.get(newGrid);
				if(_newGrid ==null)
				{
					_newGrid = new Grid(Maxnum);
					grids.put(newGrid, _newGrid);
				}

				_oldGrid.delete(oldEntity.getID());


				SecondIndex secondIndex =new SecondIndex(newEntity.getID(), _newGrid.insert(newEntity),_newGrid.Getdx(),_newGrid);
				SecondIndexList.getInstance().add(secondIndex);

				//				if(index.getBucKet().entrylst.size()==0)
				//				{
				//					System.out.print("fuck");
				//					
				//				}
				//				index.setIdx(_newGrid.Getdx());
			}
		}

	}
	private String ComputeGrid(Entity entity)
	{
		return ((int)(entity.getX()/gridWidth))+","+(int)(entity.getY()/gridlongth);

	}
}
