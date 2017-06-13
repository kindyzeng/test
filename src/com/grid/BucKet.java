package com.grid;

import java.io.Serializable;
import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.IllegalFormatCodePointException;
import java.util.List;


public class BucKet implements Serializable{
	private BucKet nextBucKet;
	private int MaxNum;
	public List<Entity>entrylst;
	private int idx;

	public int getIdx() {
		return idx;
	}



	//��������ΪXmin��Ymin��Xmin,Ymax,����˳���жϱ�����
	public List<Entity>SearchByRangebyCondition(List<Double>Rec)
	{
		return SearchByRange(this,null,Rec);
	}




	//��ѯ���е�
	public List<Entity>SearchByRangeAll()
	{
		return SearchByRange(this,null,null);
	}


	private List<Entity>SearchByRange(BucKet bucket,List<Entity>Lst,List<Double>Rec)
	{
		if(Lst==null)
		{
			Lst =new ArrayList<Entity>();

		}
		if(Rec==null)
		{
			for(int i=0;i<entrylst.size();i++)
			{
				Lst.add(entrylst.get(i));

			}
		}
		else {
			for(int i=0;i<entrylst.size();i++)
			{
				if(entrylst.get(i).isContain(Rec))
				{
					Lst.add(entrylst.get(i));
				}

			}
		}

		if(bucket.nextBucKet!=null)
		{
			return SearchByRange( bucket.nextBucKet,Lst, Rec);
		}
		else {

			return Lst;
		}
	}

	public BucKet(int maxNum) {
		super();
		MaxNum = maxNum;
		this.entrylst = new ArrayList<Entity>();
	}


	public BucKet insert(Entity entry) {

		return insert(entry,this);
	}

	private BucKet insert(Entity entry,BucKet bucket)
	{

		if(bucket.nextBucKet!=null)
		{
			return insert(entry, bucket.nextBucKet);

		}
		else {
			if(bucket.entrylst.size()==MaxNum)
			{ 
				bucket.nextBucKet = new BucKet(MaxNum);
				bucket.nextBucKet.entrylst.add(entry);
				idx = 0;
				return bucket.nextBucKet;

			}
			else {

				bucket.entrylst.add(entry);
				idx = bucket.entrylst.size()-1;
				return bucket;
			}
		}
	}

	public boolean Delete(String ID)
	{

		Entity entity= SetLastNull(this);
		if(entity == null)
			return false;
		if(this.entrylst.size()==0)
		{
			System.out.print("û��");
			return false;
		}
		else {
			PutEntity(ID,entity);

			return true;
		}

	}

	private Entity SetLastNull(BucKet _BucKet)
	{
		//�жϱ����Ƿ������ڵ�
		if(_BucKet.nextBucKet==null)
		{
			if(_BucKet.entrylst.size()>0)
			{
				Entity entity1 = _BucKet.entrylst.get(_BucKet.entrylst.size()-1);
				entrylst.remove(_BucKet.entrylst.size()-1);
				SecondIndexList.getInstance().remove(entity1.getID());
				return entity1;
			}
			else {
				return null;
			}
		}
		else
		{
			//�жϽڵ�����Ƿ������ڵ�
			if(_BucKet.nextBucKet.nextBucKet!=null)
			{
				return SetLastNull(_BucKet.nextBucKet);
			}
			else
			{
				Entity entity1 = _BucKet.nextBucKet.entrylst.get(_BucKet.nextBucKet.entrylst.size()-1);
				_BucKet.nextBucKet.entrylst.remove(_BucKet.nextBucKet.entrylst.size()-1);
				if(_BucKet.nextBucKet.entrylst.size()==0)
				{
					_BucKet.nextBucKet =null;
				}
				SecondIndexList.getInstance().remove(entity1.getID());
				return entity1;
			}
		}


	}

	private void PutEntity(String id,Entity entity)
	{
		if(!entity.getID().equals(id))
		{
			SecondIndex index = SecondIndexList.getInstance().Search(id);

			int _idx = index.getIdx();
			index.getBucKet().entrylst.set(_idx, entity);		

			index.setId(entity.getID());		
		}
	}

}
