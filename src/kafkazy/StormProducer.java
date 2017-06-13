package kafkazy;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class StormProducer {


    private static String[] sentences = new String[]{
            "the cow jumped over the moon",
            "the man went to the store and bought some candy",
            "four score and seven years ago",
            "how many apples can you eat",
    };

    public static void main(String args[]) throws InterruptedException {
        Properties props = new Properties();

        props.put("metadata.broker.list", "202.121.180.3:9092,202.121.180.4:9092,202.121.180.5:9092,202.121.180.6:9092,202.121.180.7:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
       int k =0;
        while (true) {
            for (String sentence : sentences) {
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("update", sentence);
                producer.send(data);
                System.out.println(k+++"\n");
                Thread.sleep(1);
            }
        }

    }
}
