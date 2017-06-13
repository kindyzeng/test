package zjd;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.grid.UGrid;
import com.grid.Entity;


public class getObjectServer extends ServerSocket {
	private static final int SERVER_PORT = 1010;
	public getObjectServer()throws IOException {
		super(SERVER_PORT);
		try {
			while (true) {
				System.out.println("开始了");
				Socket socket = accept();
				new CreateMyServerThread(socket);//��������ʱ����һ���̴߳���
			}
		}catch (IOException e) {
		}finally {
			close();
		}
	}

	//�߳���
	class CreateMyServerThread extends Thread {
		private Socket client;
		private BufferedReader bufferedReader;
		private PrintWriter printWriter;
		ObjectInputStream in ;
		public CreateMyServerThread(Socket s)throws IOException {
			client = s;
			//bufferedReader =new BufferedReader(new InputStreamReader(client.getInputStream()));
			
	        //�������ݵ��������������Ҫ��ObjectInputStream��ObjectOutputStream����  
	        in = new ObjectInputStream(client.getInputStream());  
	        //ObjectOutputStream out = new ObjectOutputStream(scoket.getOutputStream());  
	        
			printWriter =new PrintWriter(client.getOutputStream(),true);
			System.out.println("Client(" + getName() +") come in...");
			
			start();
		}

		public void run() {
			
			try {
				//String line ;
				
				//line = bufferedReader.readLine();
				UGrid ug;//=(UGrid)in.readObject();
				List<Entity>el;
				//try {
				
				while ((ug=(UGrid)in.readObject())!=null) {
					
					el=new ArrayList<Entity>();
					printWriter.println("continue, Client(" + getName() +")!");
					el=ug.returnAll();
					for(Entity e:el)
					{
						System.out.println("Client(" + getName() +") say: " 
								+ "ID:" + e.getID()+" X:"+e.getX()+" Y:"+e.getY());
					}		
					//ug = (UGrid)in.readObject();		
				}			
				
				//} catch (IOException e) {
					// TODO Auto-generated catch block 
					//System.out.println("!!!"+e.toString());
				//}
				//finally{
				
				printWriter.println("bye, Client(" + getName() +")!");

				System.out.println("Client(" + getName() +") exit!");

				printWriter.close();
				//bufferedReader.close();
				client.close();
				//}
			}catch (IOException | ClassNotFoundException e) {
				System.out.println("???"+e.toString());
			}
		}
	}

	public static void main(String[] args)throws IOException {
		new getObjectServer();
	}
}
