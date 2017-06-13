package zjd;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.grid.Entity;
import com.grid.UGrid;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Server extends ServerSocket {
	private static final int SERVER_PORT =171;
	private static  String topic = "" ;
	public Server(String topic)throws IOException {
		super(SERVER_PORT);
		this.topic = topic;
		try {
			while (true) {
				System.out.println("开始输入！");
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
		private PrintWriter printWriter;
		private ObjectInputStream in;
		private BufferedReader bufferedReader;

		public CreateServerThread(Socket s)throws IOException {
			client = s;
//		    bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
			printWriter =new PrintWriter(client.getOutputStream(),true);
			in = new ObjectInputStream(client.getInputStream());
			start();
		}

		public void run() {
			try {
				String line = "" ;
				Properties props = new Properties();
				props.put("zookeeper.connect", "202.121.180.3:2181,202.121.180.4:2181,202.121.180.5:2181,202.121.180.6:2181,202.121.180.7:2181");//����zk
				props.put("metadata.broker.list","202.121.180.3:9092,202.121.180.4:9092,202.121.180.5:9092,202.121.180.6:9092,202.121.180.7:9092");
				props.put("serializer.class", "kafka.serializer.StringEncoder");
				props.put("key.serializer.class", "kafka.serializer.StringEncoder");
				props.put("request.required.acks", "1");
				ProducerConfig config = new ProducerConfig(props);

				Producer<String, String> producer = new Producer<String, String>(config);
				UGrid ug;//=(UGrid)in.readObject();
				List<Entity>el;
				//try {

				while ((ug=(UGrid)in.readObject())!=null) {
					printWriter.println("continue, Client(" + getName() +")!");
					el=ug.returnAll();
					for(Entity e:el)
					{
						System.out.println("Client(" + getName() +") say: "
								+ "ID:" + e.getID()+" X:"+e.getX()+" Y:"+e.getY());
						line = e.getID()+" "+e.getX()+" "+e.getY();
						KeyedMessage<String, String> data = new KeyedMessage<String, String>(
								topic, line);
						producer.send(data);
					}
				}
				printWriter.println("bye, Client(" + getName() +")!");
				printWriter.close();
				in.close();
				client.close();
			}catch (IOException e) {
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args)throws IOException {
		new Server(args[0]);
	}
}