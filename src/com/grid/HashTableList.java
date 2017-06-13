package com.grid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HashTableList {

	private Map<String,List<SecondIndex>>map;
	public HashTableList(int mod) {
		super();
		this.mod = mod;
		map = new HashMap<String, List<SecondIndex>>();
	}
	private int mod;
	public void Insert(SecondIndex index)
	{
		InsertList(String.valueOf((Integer.parseInt(index.getId())%mod)),index);
	}
	public void Delete(String ID)
	{
		DeleteList(String.valueOf((Integer.parseInt(ID)%mod)),ID);
	}
	
	public void Update(SecondIndex index)
	{
		UpdateList(String.valueOf((Integer.parseInt(index.getId())%mod)),index);
	}
	private void InsertList(String _mod,SecondIndex index)
	{
		List<SecondIndex>aIndexs = map.get(_mod);
		if(aIndexs == null)
		{
			aIndexs = new ArrayList<SecondIndex>();
		}
		aIndexs.add(index);
		map.put(_mod, aIndexs);
	}
	private void DeleteList(String _mod,String Id)
	{
		List<SecondIndex>aIndexs = map.get(_mod);
		if(aIndexs == null)
		{
			System.out.print("û������դ�񵰰���");
			return ;
		}
		else {
			for(int i =0;i<aIndexs.size();i++)
			{
				if(aIndexs.get(i).getId().equals(Id))
				{
					aIndexs.remove(i);
					return;
				}
			}

		}
		System.out.print("û�ɴ�����դ��������");
		return;

	}
	private void UpdateList(String _mod,SecondIndex index)
	{
		List<SecondIndex>aIndexs = map.get(_mod);
		if(aIndexs == null)
		{
			System.out.print("û�����ݸ��¸�������");
			return ;
		}
		else {
			for(int i =0;i<aIndexs.size();i++)
			{
				if(aIndexs.get(i).getId().equals(index.getId()))
				{
					aIndexs.add(index);
					return;
				}
			}
		}
		System.out.print("û�ɴ����ݸ��¸�������");
		return;

	}
	public SecondIndex  Search(String ID)
	{
		return SearchList(String.valueOf((Integer.parseInt(ID)%mod)),ID);
	}
	
	private SecondIndex SearchList(String _mod,String ID)
	{
		List<SecondIndex>aIndexs = map.get(_mod);
		if(aIndexs == null)
		{
			System.out.print("û�����ݲ��������");
			return  null;
		}
		else {
			for(int i =0;i<aIndexs.size();i++)
			{
				if(aIndexs.get(i).getId().equals(ID))
				{
				
					return aIndexs.get(i);
				}
			}
		}
		System.out.print("û�ɴ����ݲ��������");
		return null;
	}
	

}
