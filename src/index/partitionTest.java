package index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

//
public class partitionTest {	

	private static double joinDistance; 

	private static int x ;
	static StringBuffer sb; 
 
	static FileWriter writer;
	static BufferedWriter bw ;
	static File f1 ;
	public static void main(String[] args) throws Exception {
		joinDistance = Double.parseDouble(args[0]);
		writer = new FileWriter(args[2],true);
		bw = new BufferedWriter(writer);
		sb= new StringBuffer("");		 
		x= 0;
		int y=0;
		String str = "";
		f1 = new File(args[1]);
		if (f1.isDirectory()&&f1.listFiles().length>0) {  
			for (File f : f1.listFiles()) {
				try {
					List<String> lines = FileUtils.readLines(f,"UTF-8");
					for (String line : lines) {

						System.out.print(y++); 			
						excute(line);
						if(x<y)
						{
							System.out.print(line+"\n"); 
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
		System.out.print(x); 
		bw.write(sb.toString());
		bw.close();
		writer.close();

	}

	private static void excute(String Str)
	{
		String points[] = Str.split("\t");
		String id = points[1];
	
		if(points[0].equals("newpoint"))
		{

			List<String>AC=ComputeAffecGrid(Double.parseDouble(points[2]),Double.parseDouble(points[3]));
			for(int i=0;i<=AC.size()-1;i++)
			{	
				x++;
				sb.append(AC.get(i)+"#"+"ADD,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])+"\n");
			}
		}
		if(points[0].equals("disappearpoint"))
		{	

			List<String>AC=ComputeAffecGrid(Double.parseDouble(points[4]),Double.parseDouble(points[5]));
			for(int i=0;i<=AC.size()-1;i++)
			{	
				x++;
				sb.append(AC.get(i)+"#"+"DELETE,"+id+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])+"\n");
			}
		}
		if(points[0].equals("point"))
		{
			List<String>ACOld=ComputeAffecGrid(Double.parseDouble(points[2]),Double.parseDouble(points[3]));
			List<String>ACNew=ComputeAffecGrid(Double.parseDouble(points[4]),Double.parseDouble(points[5]));
			List<String>Share= new ArrayList<>();
			List<String>oldonly= new ArrayList<>();
			List<String>newOnly= new ArrayList<>();
			for(int i=0;i<ACOld.size()-1;i++)
			{	
				String  oldStr = ACOld.get(i); 
				for(int j=0;j<ACNew.size()-1;j++)
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
				x++;
				sb.append(oldonly.get(k)+"#"+"DELETE,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])+"\n");
			}
			for(int k=0;k<=newOnly.size()-1;k++)
			{
				x++;
				sb.append(newOnly.get(k)+"#"+"ADD,"+id+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])+"\n");
			}
			for(int k=0;k<=Share.size()-1;k++)
			{
				x++;
				sb.append(Share.get(k)+"#"+"UPDATE,"+id+","+Double.parseDouble(points[2])+","+Double.parseDouble(points[3])+","+Double.parseDouble(points[4])+","+Double.parseDouble(points[5])+"\n");
			}
		}
	}
	//����X��Y�жϺ�JoinDistance�ж��Ƿ�intesect
	private static List<String> ComputeAffecGrid(double x,double y)
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

		return affectGrid;
	}
	private static int GetMax(int x,int y)
	{
		if(x>y)
		{
			return x;
		}
		else {
			return y;
		}
	}

	private static  int GetMin(int x,int y)
	{
		if(x<y)
		{
			return x;
		}
		else {
			return y;
		}
	}
}
