package com.grid;

import java.util.ArrayList;
import java.util.List;


public class NormalGrid {
	private List<Entity>lstEntities;
	private String gridID;

	public List<Entity>getEntities(List<Double>Rec)
	{
		if(Rec == null)
		{
			return lstEntities;
		}
		else {
			List<Entity>resEntities =new ArrayList<Entity>();
			for(int k=0;k<lstEntities.size();k++)
			{
				if(lstEntities.contains(Rec))
				{
					resEntities.add(lstEntities.get(k));
				}
			}
			return resEntities;
		}
	}
	public int GetSize()
	{
		return lstEntities.size();
	}
	public void Insert(Entity entity)
	{
		lstEntities.add(entity);
	}
	public void Update(Entity entity)
	{
		for(int k=0;k<lstEntities.size();k++)
		{

			if(lstEntities.get(k).getID().equals(entity.getID()))
			{
				lstEntities.set(k, entity);
				return;
			}
		}
		lstEntities.add(entity);
		return;

	} 

	public NormalGrid(String gridID) {
		super();
		this.gridID = gridID;
		this.lstEntities = new ArrayList<Entity>();
	}
	public void Delete(String ID)
	{
		for(int k=0;k<lstEntities.size();k++)
		{

			if(lstEntities.get(k).getID().equals(ID))
			{
				lstEntities.remove(k);
				return;
			}
			return;

		} 
	}

}
