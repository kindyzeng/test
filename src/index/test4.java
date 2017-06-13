package index;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

//
public class test4 {	

	private static double longth =468*4;
	private static double width = 476*4;

	public static double nextDouble(final double min, final double max) throws Exception {
		if (max < min) {
			throw new Exception("min < max");
		}
		if (min == max) {
			return min;
		}
		return min + ((max - min) * new Random().nextDouble());
	}
	
	public static void main(String[] args) throws Exception {

		System.out.println("fuckzy"); 


		 FileWriter logfwriter=new FileWriter(args[0]);
		 BufferedWriter logbWriter=new BufferedWriter(logfwriter);
		 StringBuffer sb= new StringBuffer("");
		 for(int i=1;i<10000000;i++)
		 {
			 double x = nextDouble(300,23500);
			 double y = nextDouble(300,23100);	
			 sb.append(i+","+x+","+y+","+(x+width)+","+(y+longth)+"\n");
			System.out.print(i+"\n");
			if(i%1000000==0)
			{
				logbWriter.write(sb.toString());
				sb= new StringBuffer("");
			}
			
		
		 }
	    logbWriter.write(sb.toString());
		 logbWriter.close();
		 logfwriter.close();
		 System.out.print("OK");
				


	}



}
