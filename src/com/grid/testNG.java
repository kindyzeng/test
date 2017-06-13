package com.grid;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;


public class testNG {
	private static File file;
	private static NormalGridIndex ugridIndex;
	
	private static File[] fs;
	public static void main(String[] args) throws Exception {

		ugridIndex =new NormalGridIndex();
		file = new File(args[0]);

		int k=0;
		List<String> lines = FileUtils.readLines(file,"UTF-8");
		long begin = System.currentTimeMillis();

		for (String line : lines) { 
			if(line.split("\t")[0].equals("newpoint"))
			{

				Entity entity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				ugridIndex.Insert(entity);
			}
			if(line.split("\t")[0].equals("point"))
			{

				if(line.split("\t")[1].equals("96263"))
				{
					System.out.print("fuck");
				}
				Entity oldentity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				Entity newentity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[4]),Double.parseDouble(line.split("\t")[5]));
				ugridIndex.Update(oldentity, newentity);
			}
			if(line.split("\t")[0].equals("disappearpoint"))
			{
				Entity entity =new Entity((line.split("\t")[1]),Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				ugridIndex.Delete(entity);

			}
			k++;
			System.out.print(k+"\n");
		}
		//		}
		long end = System.currentTimeMillis();
		List<Entity>lst= ugridIndex.RangeQuery(10, 20, 3000, 4000);

		if(lst!=null)
		{
			for(int i=0;i<lst.size();i++)
			{
				System.out.print(lst.get(i).getID()+"\n");
			}
		}

//		file = new File(args[1]);
//		
//		for (String line : lines) { 
//			lines = FileUtils.readLines(file,"UTF-8");
//			Double Xmin = Double.valueOf(line.split(",")[1]);
//			Double Ymin = Double.valueOf(line.split(",")[2]);
//			Double Xmax =Double.valueOf(line.split(",")[3]);
//			Double Ymax = Double.valueOf(line.split(",")[4]);
//			ugrid.RangeQuery(Xmin, Ymin, Xmax, Ymax);
//		}


	}
}
