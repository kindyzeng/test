package zjd;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class LocalServer extends ServerSocket {
	private static final int SERVER_PORT =171;
	   private static  String topic ;
	public LocalServer()throws IOException {
		super(SERVER_PORT);
		try {
			while (true) {
				System.out.println("开始了！");
				Socket socket = accept();
				new CreateServerThread(socket);//��������ʱ����һ���̴߳���
			}
		}catch (IOException e) {
		}finally {
			close();
		}
	}

	//�߳���
	class CreateServerThread extends Thread {
		private Socket client;
		private BufferedReader bufferedReader;
		private PrintWriter printWriter;

		public CreateServerThread(Socket s)throws IOException {
			client = s;

			bufferedReader =new BufferedReader(new InputStreamReader(client.getInputStream()));

			printWriter =new PrintWriter(client.getOutputStream(),true);
			System.out.println("Client(" + getName() +") come in...");  


			start();
		}

		public void run() {
			try {
				String line ;
//				// 设置配置属性
//				Properties props = new Properties();
//				props.put("zookeeper.connect", "202.121.180.85:2181,202.121.180.82:2181,202.121.180.83:2181");//声明zk
//				props.put("metadata.broker.list","202.121.180.85:9092,202.121.180.82:9092,202.121.180.83:9092");
//				props.put("serializer.class", "kafka.serializer.StringEncoder");
//				// key.serializer.class默认为serializer.class
//				props.put("key.serializer.class", "kafka.serializer.StringEncoder");
//				// 可选配置，如果不配置，则使用默认的partitioner
//				//		props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
//				// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
//				// 值为0,1,-1,可以参考
//				// http://kafka.apache.org/08/configuration.html
//				props.put("request.required.acks", "1");
//				ProducerConfig config = new ProducerConfig(props);
//
//				// 创建producer
//				Producer<String, String> producer = new Producer<String, String>(config);



				line = bufferedReader.readLine();
				while (line!=null) {
					printWriter.println("continue, Client(" + getName() +")!");
					//        		      System.out.println("fuck4");
					System.out.println("Client(" + getName() +") say: " + line);
					line = bufferedReader.readLine();

				}
				printWriter.println("bye, Client(" + getName() +")!");

				System.out.println("Client(" + getName() +") exit!");
				printWriter.close();
				bufferedReader.close();
				client.close();
			}catch (IOException e) {
			}
		}
	}

	public static void main(String[] args)throws IOException {
		new LocalServer();
	}
}