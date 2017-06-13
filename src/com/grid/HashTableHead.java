package com.grid;//import javax.naming.directory.SearchControls;
//
//
//public class HashTableHead {
//	private HashTableEntity nextEntity;
//
//	public HashTableHead() {
//		super();
//
//	}
//	public HashTableEntity getNextEntity() {
//		return nextEntity;
//	}
//	public void setNextEntity(HashTableEntity nextEntity) {
//		this.nextEntity = nextEntity;
//	}
//	public void Insert(SecondIndex _index)
//	{		
//
//		if(this.nextEntity == null)
//		{
//			this.nextEntity = new HashTableEntity(_index);
//		}	
//		else {
//			InsertEntity(this.nextEntity,_index);
//		}
//	}
//	private void InsertEntity(HashTableEntity entity,SecondIndex _index)
//	{
//		if(entity.getNextEntity()!=null)
//		{
//			InsertEntity(entity.getNextEntity(),_index);
//		}
//		else {
//			HashTableEntity _entity = new HashTableEntity(_index);
//			entity.setNextEntity(_entity);
//		}
//	}
//	
//	public void Delete(String id)
//	{
//		if(this.nextEntity!=null)
//		{
//			
//			if(this.getNextEntity().getNextEntity()!=null)
//			{
//				if(this.getNextEntity().getIndex().getId().equals(id))
//				{
//					this.setNextEntity(this.getNextEntity().getNextEntity());
//				}
//				else {
//					Delete(this.getNextEntity(),id);
//				}
//			}
//			else {
//				if(this.getNextEntity().getIndex().getId().equals(id))
//				{
//					this.setNextEntity(null);
//				}
//				else {
//					System.out.print("û�����IDɾ������"+id+"\n");
//				}
//			}
//			
//		}
//		else {
//			System.out.print("û������ɾ��������"+"\n");
//		}
//	}
//	private void Delete(HashTableEntity entity,String id)
//	{
//		if(entity.getNextEntity().getNextEntity()!=null)
//		{
//			if(entity.getNextEntity().getIndex().getId().equals(id))
//			{
//				entity.setNextEntity(entity.getNextEntity().getNextEntity());
//				return;
//			}
//			else {
//				Delete(entity.getNextEntity(),id);
//			}
//		}
//		else {
//			if(entity.getNextEntity().getIndex().getId().equals(id))
//			{
//				entity.setNextEntity(null);
//				return;
//			}
//			else {
//				System.out.print("û�����IDɾ������"+id+"\n");
//				return ;
//			}
//
//		}
//
//	}
//	
//	public void Search(String ID)
//	{
//		if(this.nextEntity==null)
//		{
//			System.out.print("û������ɾ��������"+"\n");
//		}
//		else {
//			
//		}
//	}
//	private void SearchEntity(HashTableEntity entity,String id)
//	{
//		if()
//	}
//}
