package com.grid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class NormalGridIndex {
	private Map<String, NormalGrid>map;
	private static double gridWidth=238;
	private static double gridlongth =234;


	private List<Entity> GetEntityByGrid(int row ,int col,List<Double>rec)
	{
		NormalGrid grid = map.get(col+","+row);
		if(grid !=null)
		{
			return grid.getEntities(rec);
		}
		return null;
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
	public void Delete(Entity entity)
	{
		String guidString = ComputeGrid(entity);
		NormalGrid grid = map.get(guidString);
		if(grid == null)
		{
			return;
		}
		grid.Delete(entity.getID());
		if(grid.GetSize() == 0)
		{
			map.remove(guidString);
		}
	}

	public NormalGridIndex() {

		super();
		map = new HashMap<String, NormalGrid>(); 
	}

	public void Update(Entity oldEntity,Entity newEntity)
	{

		String oldGuid = ComputeGrid(oldEntity);
		String newGuid = ComputeGrid(newEntity);

		if(oldGuid.equals(newGuid))
		{
			NormalGrid newGrid = map.get(newGuid);
			if(newGrid!=null)
			{
				newGrid.Update(newEntity);
			}
			else {
				Insert(newEntity);
			}
		}
		else {
			NormalGrid oldgrid = map.get(oldGuid);
			NormalGrid newGrid = map.get(newGuid);
			oldgrid.Delete(oldGuid);
			newGrid.Insert(newEntity);
		}


	}


	public void Insert(Entity entity)
	{
		String guidString = ComputeGrid(entity);
		NormalGrid grid = map.get(guidString);
		if(grid == null)
		{
			grid = new NormalGrid(guidString);
		}
		grid.Insert(entity);
		map.put(guidString, grid);
	}

	private String ComputeGrid(Entity entity)
	{
		return ((int)(entity.getX()/gridWidth))+","+(int)(entity.getY()/gridlongth);
	}
}
