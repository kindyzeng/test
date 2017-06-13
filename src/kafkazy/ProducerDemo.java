package kafkazy;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 详细可以参考：https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 * @author Fung
 *
 */
public class ProducerDemo {
	public static void main(String[] args) {
		Random rnd = new Random();
		int events=100;
		// 设置配置属性
		Properties props = new Properties();
		props.put("zookeeper.connect", "202.121.180.3:2181,202.121.180.4:2181,202.121.180.5:2181,202.121.180.6:2181,202.121.180.7:2181");//声明zk  
		props.put("metadata.broker.list","202.121.180.3:9092,202.121.180.4:9092,202.121.180.5:9092,202.121.180.6:9092,202.121.180.7:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// key.serializer.class默认为serializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// 可选配置，如果不配置，则使用默认的partitioner
		//		props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
		// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
		// 值为0,1,-1,可以参考
		// http://kafka.apache.org/08/configuration.html
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		// 创建producer
		Producer<String, String> producer = new Producer<String, String>(config);
		//       产生并发送消息
		//如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
		for(int i=0;i<10000;i++)
		{
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"query", args[0]);
			producer.send(data);
		}

		// 关闭producer
		producer.close();
		System.out.print("OK");
	}
}