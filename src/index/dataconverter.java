package index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

//
public class dataconverter {	
	public static void main(String[] args) throws Exception {
		StringBuffer sb= new StringBuffer("");
		FileReader reader = new FileReader(args[0]);
		BufferedReader br = new BufferedReader(reader);
		String str = null;
		Map<String, String> teMap = new HashMap<String, String>();
		int k =0;
		String tempTime = "0";
	    FileWriter writer = new FileWriter(args[1]+tempTime+".txt");
        BufferedWriter bw  = new BufferedWriter(writer);
		while((str = br.readLine()) != null) {
			String a[] = str.split("\t");
			String name =a[0];
			String time = a[4];
			if(!tempTime.equals(time))
			{
				tempTime = time;
		      bw.write(sb.toString());  
		      sb = new StringBuffer("");
		      bw.close();
		       writer.close();
		     writer = new FileWriter(args[1]+tempTime+".txt");
		     bw = new BufferedWriter(writer);
			}
			if(a[0].equals("newpoint"))
			{
				String string = a[1]+"\t"+a[5]+"\t"+a[6];
				sb.append(a[0]+"\t"+string);
				sb.append("\n");
				teMap.put(a[1], string);
			}
			if(a[0].equals("point")||a[0].equals("disappearpoint"))
			{
				String string1 = a[1]+"\t"+a[5]+"\t"+a[6];
				String value = teMap.get(a[1]);
				String b[] = value.split("\t");
				if(value == null)
				{
					System.out.print("fuck");
				}
				else {
					String string = a[1]+"\t"+b[1]+"\t"+b[2]+"\t"+a[5]+"\t"+a[6];	
					if(a[0].equals("point"))
					{
						teMap.remove(a[1]);
						teMap.put(a[1], string1);
					}
					if(a[1].equals("disappearpoint"))
					{
						teMap.remove(a[1]);
					}
					sb.append(a[0]+"\t"+string);
					sb.append("\n");
					
				}
			}

			System.out.print(k++);  
			System.out.print("\n");  
		}
	    bw.write(sb.toString());  
		br.close();
		reader.close();
        bw.close();
        writer.close();
	}

}
