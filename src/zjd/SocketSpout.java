package zjd;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.grid.Entity;
import com.grid.UGrid;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 金迪 on 2017/5/31.
 */
public class SocketSpout extends BaseRichSpout{
    private static final int SERVER_PORT = 1010;

    SpoutOutputCollector collector;
    ServerSocket serverSocket;
    Socket socket;
    ObjectInputStream in ;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            socket = serverSocket.accept();
            in = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        //判断socket是否为空
        if(socket == null){
            try {
                serverSocket = new ServerSocket(SERVER_PORT);
                socket = serverSocket.accept();
                in = new ObjectInputStream(socket.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            new CreateMyServerThread(socket);
        }catch (IOException e) {
            System.out.println("问题："+e.toString());
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("List"));
    }

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

                start();
            }

            public void run() {

                try {
                    UGrid ug;//=(UGrid)in.readObject();
                    List<Entity>el;
                    //try {

                    while ((ug=(UGrid)in.readObject())!=null) {

                        el=new ArrayList<Entity>();
                        printWriter.println("continue, Client(" + getName() +")!");
                        el=ug.returnAll();
                        collector.emit(new Values(el));
                    }
                    client.close();
                    //}
                }catch (IOException | ClassNotFoundException e) {
                    System.out.println("???"+e.toString());
                }
            }
    }

}
