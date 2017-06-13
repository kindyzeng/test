import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * ��ϸ���Բο���https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 * @author Fung
 *
 */
public class ProducerDemo {
	public static void main(String[] args) {
		Random rnd = new Random();
		int events=100;
		// ������������
		Properties props = new Properties();
		props.put("zookeeper.connect", "202.121.180.85:2181,202.121.180.82:2181,202.121.180.83:2181");//����zk
		props.put("metadata.broker.list","202.121.180.85:9092,202.121.180.82:9092,202.121.180.83:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// key.serializer.classĬ��Ϊserializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// ��ѡ���ã���������ã���ʹ��Ĭ�ϵ�partitioner
		//		props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
		// ����acknowledgement���ƣ�������fire and forget�����ܻ��������ݶ�ʧ
		// ֵΪ0,1,-1,���Բο�
		// http://kafka.apache.org/08/configuration.html
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		// ����producer
		Producer<String, String> producer = new Producer<String, String>(config);
		//       ������������Ϣ
		//���topic�����ڣ�����Զ�������Ĭ��replication-factorΪ1��partitionsΪ0
		for(int i=0;i<10000;i++)
		{
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"zy", args[0]);
			producer.send(data);
		}

		// �ر�producer
		producer.close();
		System.out.print("OK");
	}
}