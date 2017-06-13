package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import model.Point_Mdl;

//
public class test {	



	public static void main(String[] args) throws Exception {

		System.out.println("fuck"); 
		// 设置配置属性
				Properties props = new Properties();
		        props.put("metadata.broker.list", "202.121.180.100:9092,202.121.180.101:9092,202.121.180.102:9092,202.121.180.103:9092,202.121.180.104:9092,202.121.180.105:9092,202.121.180.106:9092");
				props.put("serializer.class", "kafka.serializer.StringEncoder");
//		 key.serializer.class默认为serializer.class
				props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		//		// 可选配置，如果不配置，则使用默认的partitioner
				props.put("partitioner.class", "index.TestPartition");
		// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
		// 值为0,1,-1,可以参考
		// http://kafka.apache.org/08/configuration.html
				props.put("request.required.acks", "1");
			ProducerConfig config = new ProducerConfig(props);
			int k=0;
		// 创建producer
			Producer<String, String> producer = new Producer<String, String>(config);
				FileReader reader = new FileReader(args[0]);
				BufferedReader br = new BufferedReader(reader);
				String str = null;
				while((str = br.readLine()) != null) {
			
					KeyedMessage<String, String> data = new KeyedMessage<String, String>(
							args[1],String.valueOf(k),str);			
					System.out.print(k+++"\n");
					producer.send(data);	
				}
			
		// 产生并发送消息
//		long start=System.currentTimeMillis();		
				System.out.print("OK");
				
				producer.close();


	}



}
