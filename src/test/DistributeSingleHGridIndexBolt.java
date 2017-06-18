package test;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import model.HGrid;

import com.grid.Entity;
import com.grid.NormalGridIndex;
import com.grid.UGrid;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import scala.collection.generic.BitOperations;


public class DistributeSingleHGridIndexBolt extends BaseBasicBolt {
	public UGrid getGridIndex() {
		return gridIndex;
	}

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	UGrid gridIndex;
	Date curDate;
	int k;//计数器

	@Override 
	public void prepare(Map conf,TopologyContext context)
	{
		k = 0;
		gridIndex =new UGrid();
		curDate = new Date(System.currentTimeMillis());//获取当前时间
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(input.getSourceComponent().equalsIgnoreCase("Print"))
		{
//			k++;
//			if(k == 500000){
//                try {
//                    String result ="";
//					Socket socket = new Socket("221.12.174.182",110);
//                    socket.setSoTimeout(60000);
//
//                    //传输实体
//                    ObjectOutputStream outputStream  = new ObjectOutputStream(socket.getOutputStream());
//                    BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//                    outputStream.writeObject(gridIndex);
//                    outputStream.flush();
//
//                    String re = bufferedReader.readLine();
//                    System.out.println("Server say"+re);
//
//                    outputStream.close();
//                    socket.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                k = 0;
//			}
//			Date dateNow = new Date(System.currentTimeMillis());

//            if((dateNow.getTime() - curDate.getTime()) >= 180 * 1000){
//                curDate = dateNow;
//                //传输
//                Socket socket = null;
//                try {
//                    String result ="";
//                    socket = new Socket("221.12.174.182",110);
//                    socket.setSoTimeout(60000);
//
//                    //传输实体
//                    ObjectOutputStream outputStream  = new ObjectOutputStream(socket.getOutputStream());
//                    BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//                    outputStream.writeObject(gridIndex);
//                    outputStream.flush();
//
//                    String re = bufferedReader.readLine();
//                    System.out.println("Server say"+re);
//
//                    outputStream.close();
//                    socket.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
			String line = input.getString(1);
			String area = "",id = "";

			if(line.split("\t")[0].equals("newpoint"))
			{
				area = line.split("\t")[4];
				id = String.valueOf(Integer.parseInt(line.split("\t")[1]) + Integer.parseInt(area) * 10000000);

				Entity entity =new Entity(id,Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				gridIndex.Insert(entity);

//				collector.emit("data",new Values(entity));
			}
			if(line.split("\t")[0].equals("point"))
			{
				area = line.split("\t")[6];
				id = String.valueOf(Integer.parseInt(line.split("\t")[1]) + Integer.parseInt(area) * 10000000);
				if(line.split("\t")[1].equals("96263"))
				{
					System.out.print("fuck");
				}
				Entity oldentity =new Entity(id,Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				Entity newentity =new Entity(id,Double.parseDouble(line.split("\t")[4]),Double.parseDouble(line.split("\t")[5]));
				gridIndex.Update(oldentity, newentity);

//				collector.emit("data",new Values(newentity));
			}
			if(line.split("\t")[0].equals("disappearpoint"))
			{
				area = line.split("\t")[4];
				id = String.valueOf(Integer.parseInt(line.split("\t")[1]) + Integer.parseInt(area) * 10000000);

				Entity entity =new Entity(id,Double.parseDouble(line.split("\t")[2]),Double.parseDouble(line.split("\t")[3]));
				gridIndex.Disappear(entity);

//				collector.emit("data",null);
			}


		}
		if(input.getSourceComponent().equalsIgnoreCase("QuerySpout"))
		{
			String line =  input.getString(0);

//			BufferedWriter out = null;
//			try {
//				FileWriter fileWriter = new FileWriter("/usr/local/software/2.txt",true);
//				out = new BufferedWriter(fileWriter);
//				out.write(line+"\r\n");
//				out.flush(); // 把缓存区内容压入文件
//				out.close(); // 最后记得关闭文件
//			} catch (IOException e) {
//				e.printStackTrace();
//			}

			String queryID = line.split(",")[0];
			Double Xmin = Double.valueOf(line.split(",")[1]);
			Double Ymin = Double.valueOf(line.split(",")[2]);
			Double Xmax =Double.valueOf(line.split(",")[3]);
			Double Ymax = Double.valueOf(line.split(",")[4]);
			List<Entity>  qresult = gridIndex.RangeQuery(Xmin, Ymin, Xmax, Ymax);
			String resultStr="";
			for(Entity e : qresult)
			{
				resultStr+=e.getID()+" "+e.getX()+" "+e.getY()+",";
			}
			resultStr.substring(0, resultStr.length()-2);
			collector.emit(new Values(queryID,resultStr));
		}

		if(input.getSourceComponent().equalsIgnoreCase("DataSpout")){
//			List<Entity> el = input.getValues();
            String line = input.getString(0);
			String id = line.split(" ")[0];
			Entity newentity = new Entity((line.split(" ")[0]),Double.parseDouble(line.split(" ")[1]),Double.parseDouble(line.split(" ")[2]));
			gridIndex.Update(id,newentity);
		}

	}


	public  DistributeSingleHGridIndexBolt() {
		super();
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("QueryID","Result"));
		declarer.declareStream("data",new Fields("data"));
	} 
}
