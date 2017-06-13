package com.grid;

import java.util.ArrayList;
import java.util.List;


public  class SecondIndexList {
//Hash±íµÄÈ¡Óà
    private static int  mod= 100;
	public static SecondIndexList getInstance() {  
		if(SecondIndexHash==null)
		{
			SecondIndexHash = new HashTableList(mod);
		}
		if (instance == null) {  
			instance = new SecondIndexList(); 

		}  
		return instance;  
	}  
	private static SecondIndexList instance;  
	private static  HashTableList SecondIndexHash ;
	public void add(SecondIndex index)
	{
//		for(int i=0;i<SecondIndexl.size();i++)
//		{
//			if(SecondIndexl.get(i).getId().equals(index.getId()))
//			{
//				SecondIndexl.add(i, index);
//				return;
//			}
//		}
		SecondIndexHash.Insert(index);
	}
	public void update(SecondIndex index)
	{
		SecondIndexHash.Update(index);
	}
	public void remove(String ID)
	{
		SecondIndexHash.Delete(ID);

	}

//	public int GetSize()
//	{
//		return SecondIndexl.size();
//	}
	public SecondIndex Search(String ID)
	{
		return SecondIndexHash.Search(ID);
	}
}
