package zjd;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.util.parsing.combinator.testing.Str;

import java.io.*;
import java.util.Properties;

/**
 * Created by 金迪 on 2017/5/22.
 * 向kafka中输入数据
 */
public class Producer {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.put("zookeeper.connect","192.168.1.100:2181,192.168.1.101:2181,192.168.1.102:2181");
        prop.put("metadata.broker.list","192.168.1.100:9092,192.168.1.101:9092,192.168.1.102:9092");
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        prop.put("key.serializer.class", "kafka.serializer.StringEncoder");
        prop.put("request.required.acks", "1");


        ProducerConfig config = new ProducerConfig(prop);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(config);
        if(args != null && args.length>0){
            String path = args[0];
            String topic = args[1];
            File file = new File(path);
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), "utf-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = bufferedReader.readLine())!=null){
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                        topic, line);
                producer.send(data);
                System.out.print(line);
            }
            producer.close();
            System.out.println("OK!干完了");
        }
        else {
            String path = "D:/zy.txt";
            File file = new File(path);
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), "utf-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = bufferedReader.readLine())!=null){
                System.out.println(line);
            }
            System.out.println("OK!干完了");
        }
    }

}
