package zjd;
 
import com.grid.Entity;
import com.grid.UGrid;

import java.io.*;
import java.net.Socket;
  
public class SocketClient {
    public static void main(String[] args) {
        try {
            System.out.println("第1步");
            Socket socket =new Socket("202.196.76.108",20001);
            socket.setSoTimeout(60000);
            System.out.println("第2步");

//            //传输实体
//            UGrid uGrid = new UGrid();
//            Entity entity = new Entity("1",100,100);
//            uGrid.Insert(entity);
//            entity = new Entity("2",200,200);
//            uGrid.Insert(entity);
//            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//            BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//            out.writeObject(uGrid);
//            out.flush();
//
//            String re = bufferedReader.readLine();
//            System.out.println("Server say"+re);
//
//            out.close();




            PrintWriter printWriter =new PrintWriter(socket.getOutputStream(),true);
            BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("第3步");

           String result ="";
            int i =0;
            BufferedReader sysBuff =new BufferedReader(new InputStreamReader(new FileInputStream("D:/zy.txt"), "UTF-8"));

          	String a = "";
            while( (a=sysBuff.readLine())!= null){

                printWriter.println(a);
                printWriter.flush();
                result = bufferedReader.readLine();

                System.out.println("Server say : " + result+" "+i++);
            }
            printWriter.println("bye");
            printWriter.close();
            bufferedReader.close();
            socket.close();
        }catch (Exception e) {
            System.out.println("Exception:" + e);
        }
    }
}