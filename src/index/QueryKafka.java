package index;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import model.Point_Mdl;

//
public class QueryKafka {	




	public static void main(String[] args) throws Exception {

		System.out.println("fuck"); 
		// ������������
		Properties props = new Properties();
		props.put("metadata.broker.list", "202.121.180.100:9092,202.121.180.101:9092,202.121.180.102:9092,202.121.180.103:9092,202.121.180.104:9092,202.121.180.105:9092,202.121.180.106:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//		 key.serializer.classĬ��Ϊserializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		//		// ��ѡ���ã���������ã���ʹ��Ĭ�ϵ�partitioner
		//				props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
		// ����acknowledgement���ƣ�������fire and forget�����ܻ��������ݶ�ʧ
		// ֵΪ0,1,-1,���Բο�
		// http://kafka.apache.org/08/configuration.html
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);	
		// ����producer
		for(int i=0;i<100000;i++)
		{		
			System.out.println(i+"\n"); 
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"query1",args[0]);	
			producer.send(data);
		}




		// ������������Ϣ
		//		long start=System.currentTimeMillis();		


		producer.close();
		System.out.print("OK");

	}
	
	private  static String MakeStr(int i)
	{
         String aString ="";
         aString +=i;
         int max=10000;
         int min=1000; 
         Random random = new Random();
         int xmin = random.nextInt(max)%(max-min+1) + min;
         int ymin = random.nextInt(max)%(max-min+1) + min;
         int xmax = xmin +20;
         int ymax = xmax +20;	
         aString += ","+xmin+","+ymin+","+xmax+","+ymax;
        return aString;
	}


}
