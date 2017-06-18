package zjd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TRSocket extends ServerSocket {
    private static final int SERVER_PORT =100;
    private static  String topic ;
    private static long time1,time2;
    private static int i,k = 1;
    
    private static BufferedReader trread;
    private static PrintWriter trWriter;
    private static Socket trsocket;
    
    public TRSocket()throws IOException {
        super(SERVER_PORT);
        try {
            while (true) {
                System.out.println("开始了呀！");
                Socket socket = accept();
                new CreateServerThread(socket);//锟斤拷锟斤拷锟斤拷锟斤拷时锟斤拷锟斤拷一锟斤拷锟竭程达拷锟斤拷
            }
        }catch (IOException e) {
        }finally {
            close();
        }
    }

    //锟竭筹拷锟斤拷
    class CreateServerThread extends Thread {
        private Socket client;
        private BufferedReader bufferedReader;
        private PrintWriter printWriter;
        
        

        

        public CreateServerThread(Socket s)throws IOException {
            client = s;

            bufferedReader =new BufferedReader(new InputStreamReader(client.getInputStream()));

            printWriter =new PrintWriter(client.getOutputStream(),true);
            System.out.println("Client(" + getName() +") come in...");
            i = 0;
            time1 = System.currentTimeMillis();
            start();
        }

        public void run() {
            try {
                String line ;

            


                line = bufferedReader.readLine();
                while (line!=null) {
                    printWriter.println("continue, Client(" + getName() +")!");
                    //        		      System.out.println("fuck4");
                    System.out.println("Client(" + getName() +") say: " + line);
                    trWriter.println(line);
                    trWriter.flush();
                    String result = trread.readLine();
            
                    time2 = System.currentTimeMillis();
                    i++;
                    if((time2-time1)>=5*1000){
                        System.out.println(k * 5+" "+i);
                        time1 = time2;k++;
                    }

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
    	trsocket =new Socket(args[0],Integer.parseInt(args[1]));
    	trsocket.setSoTimeout(60000);
    	trWriter =new PrintWriter(trsocket.getOutputStream(),true);
        trread =new BufferedReader(new InputStreamReader(trsocket.getInputStream()));
    	new TRSocket();
    	trWriter.close();
    	trread.close();
    	trsocket.close();
        
    }
}