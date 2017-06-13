package test;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
//23800*23400
public  class UpdatePartition extends BaseBasicBolt {
	private double joinDistance;                                                   
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//

		String[] points  = input.getString(0).split("\t");
		String id = points[1];
        
	
		if(points[0].equals("newpoint"))
		{

			List<String>AC=ComputeAffecGrid(Double.parseDouble(points[2]),Double.parseDouble(points[3]));
			for(int i=0;i<=AC.size()-1;i++)
			{	
				collector.emit(new Values(AC.get(i),"ADD,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])));
			}
		}
		if(points[0].equals("disappearpoint"))
		{	
			
			List<String>AC=ComputeAffecGrid(Double.parseDouble(points[4]),Double.parseDouble(points[5]));
			for(int i=0;i<=AC.size()-1;i++)
			{	
				collector.emit(new Values(AC.get(i),"DELETE,"+id+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])));
			}
		}
		if(points[0].equals("point"))
		{
			List<String>ACOld=ComputeAffecGrid(Double.parseDouble(points[2]),Double.parseDouble(points[3]));
			List<String>ACNew=ComputeAffecGrid(Double.parseDouble(points[4]),Double.parseDouble(points[5]));
			List<String>Share= new ArrayList<>();
			List<String>oldonly= new ArrayList<>();
			List<String>newOnly= new ArrayList<>();
			for(int i=0;i<=ACOld.size()-1;i++)
			{	
				String  oldStr = ACOld.get(i); 
				for(int j=0;j<=ACNew.size()-1;j++)
				{	
					if(oldStr.equals(ACNew.get(j)))
					{
						Share.add(oldStr);
					}
				}
				
			}
			for(int i=0;i<=ACOld.size()-1;i++)
			{
				if(!Share.contains(ACOld.get(i)))
				{
					oldonly.add(ACOld.get(i));
				}
			}
			for(int i=0;i<=ACNew.size()-1;i++)
			{
				if(!Share.contains(ACNew.get(i)))
				{
					newOnly.add(ACNew.get(i));
					
				}
			}
			for(int k=0;k<=oldonly.size()-1;k++)
			{
				collector.emit(new Values(oldonly.get(k),"DELETE,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])));
			}
			for(int k=0;k<=newOnly.size()-1;k++)
			{
				collector.emit(new Values(newOnly.get(k),"ADD,"+id+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])));
			}
			for(int k=0;k<=Share.size()-1;k++)
			{
				collector.emit(new Values(Share.get(k),"UPDATE,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])));
			}
		}

	}
	private int GetMax(int x,int y)
	{
		if(x>y)
		{
			return x;
		}
		else {
			return y;
		}
	}
	
	private int GetMin(int x,int y)
	{
		if(x<y)
		{
			return x;
		}
		else {
			return y;
		}
	}
	//根据X、Y判断和JoinDistance判断是否intesect
	private List<String> ComputeAffecGrid(double x,double y)
	{
	
		List<String>  affectGrid = new ArrayList<String>();
		int maxrow = GetMin((int)(y+joinDistance)/234,100);
		int maxcol =GetMin((int)(x+joinDistance)/238,100);
		int minrow = GetMax(0,(int)(y-joinDistance)/234);
		int mincol =GetMax(0,(int)(x-joinDistance)/238);
		
	    for(int i=mincol;i<=maxcol;i++)
	    	for(int j =minrow;j<=maxrow;j++)
	    	{
	    		affectGrid.add(j+","+i);
	    	}
//		affectGrid.add(row+","+col);
//		double centerY = row*234+117;
//		double centerX = 238*col+119;
//		int testcol  = col+Positive(x-centerX);
//		int testrow  = row+Positive(y-centerY);
//		int testcol2  = col+Positive2(x-centerX);
//		int testrow2 = row+Positive2(y-centerY);
//		double testColDistance = Math.abs(testcol*238-x);
//		double testRowDistance = Math.abs(testrow*234-y);
//		boolean flagcol=false;
//		if(testcol2<100&&testcol2>=0)
//		{
//			flagcol =true;
//		}
//		boolean flagrow=false;
//		if(testrow2<100&&testrow2>=0)
//		{
//			flagrow =true;
//		}
//		if(testColDistance<joinDistance&&flagcol)
//		{
//			affectGrid.add(row+","+testcol2); 
//		}
//		if(testRowDistance<joinDistance&&flagrow)
//		{
//			affectGrid.add(testrow2+","+col);
//
//		}
//		if(flagrow&&flagcol&&Math.sqrt(Math.pow(testColDistance,2)+Math.pow(testRowDistance,2))<joinDistance)
//		{
//			affectGrid.add(testrow2+","+testcol2);
//		}
		return affectGrid;
	}
	//判断正负号
	private   int Positive2(double a)
	{
		if(a>0)
		{
			return 1;
		}
		else
		{
			return  -1;
		}
	}
	//判断正负号
	private   int Positive(double a)
	{
		if(a>0)
		{
			return 1;
		}
		else
		{
			return  0;
		}
	}
	public UpdatePartition(double joinDistance) {
		super();
		this.joinDistance = joinDistance;
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GridIndex","Point"));
	} 
}