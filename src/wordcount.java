import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class wordcount {
	// ����һ����ͷ�����ڲ������ݡ�����̳���BaseRichSpout  
    public static class RandomSentenceSpout extends BaseRichSpout {  
        SpoutOutputCollector _collector;  
        Random _rand;  
          
        @Override  
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
            _collector = collector;  
            _rand = new Random();  
        }  
          
        @Override  
        public void nextTuple(){

            // ˯��һ��ʱ����ٲ���һ������  
            Utils.sleep(100);  
              
            // ��������  
            String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",  
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };  
              
            // ���ѡ��һ������  
            String sentence = sentences[_rand.nextInt(sentences.length)];  
              
            // ����þ��Ӹ�Bolt  
            _collector.emit(new Values(sentence));  
        }  
          
        // ȷ�Ϻ���  
        @Override  
        public void ack(Object id){  
        }  
          
        // ����ʧ�ܵ�ʱ�����  
        @Override  
        public void fail(Object id){  
        }  
          
        @Override  
        public void declareOutputFields(OutputFieldsDeclarer declarer){  
            // ����һ���ֶ�word  
            declarer.declare(new Fields("word"));  
        }  
    }  
      
    // �����Bolt�����ڽ������з�Ϊ����  
    public static class SplitSentence extends BaseBasicBolt{  
        @Override  
        public void execute(Tuple tuple, BasicOutputCollector collector){
            // ���յ�һ������  
            String sentence = tuple.getString(0);  
            // �Ѿ����и�Ϊ����  
            StringTokenizer iter = new StringTokenizer(sentence);  
            // ����ÿһ������  
            while(iter.hasMoreElements()){  
                collector.emit(new Values(iter.nextToken()));  
            }  
        }  
          
        @Override  
        public void declareOutputFields(OutputFieldsDeclarer declarer){  
            // ����һ���ֶ�  
            declarer.declare(new Fields("word"));  
        }  
    }  
      
    // ����һ��Bolt�����ڵ��ʼ���  
    public static class WordCount extends BaseBasicBolt {  
        Map<String, Integer> counts = new HashMap<String, Integer>();  
          
        @Override  
        public void execute(Tuple tuple, BasicOutputCollector collector){  
            // ����һ������  
            String word = tuple.getString(0);  
            // ��ȡ�õ��ʶ�Ӧ�ļ���  
            Integer count = counts.get(word);  
            if(count == null)  
                count = 0;  
            // ��������  
            count++;  
            // �����ʺͶ�Ӧ�ļ�������map��  
            counts.put(word,count);

//            File file = new File("/usr/local/software/2.txt");
//            if(!file.exists()){
//                file.setWritable(true,false);
//                try {
//                    file.createNewFile();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
            BufferedWriter out = null;
            try {
                FileWriter fileWriter = new FileWriter("/usr/local/software/1.txt",true);
                out = new BufferedWriter(fileWriter);
                out.write(word+"\n");
                out.write(count+"\n");
                out.flush(); // 把缓存区内容压入文件
                out.close(); // 最后记得关闭文件
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(word + "  " +count);  
            // ���͵��ʺͼ������ֱ��Ӧ�ֶ�word��count��  
            collector.emit(new Values(word, count));
        }

        @Override  
        public void declareOutputFields(OutputFieldsDeclarer declarer){  
            // ���������ֶ�word��count  
            declarer.declare(new Fields("word","count"));  
        }  
    }  
    public static void main(String[] args) throws Exception   
    {
        System.out.println("hello word!fuck xiaozemaliya");

        File file = new File("/usr/local/software/1.txt");
        if(!file.exists()){
            file.setWritable(true,false);
            file.createNewFile();
        }

        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("/usr/local/software/1.txt"));
            out.write("ganwo"+"\n");
            out.write("ganwo11"+"\n");
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }


        // ����һ������  
        TopologyBuilder builder = new TopologyBuilder();  
        // ����Spout�����Spout�����ֽ���"Spout"�����ò��ж�Ϊ5  
        builder.setSpout("Spout", new RandomSentenceSpout(), 5);  
        // ����slot������split�������ж�Ϊ8������������Դ��spout��  
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("Spout");  
        // ����slot������count��,�㲢�ж�Ϊ12������������Դ��split��word�ֶ�  
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));  
          
        Config conf = new Config();  
        conf.setDebug(false);
//          
        //if(args != null && args.length > 0){  
        //if(false){  
        //  conf.setNumWorkers(3);  
        //  StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        //}else{  
//            conf.setMaxTaskParallelism(3);  
              
            // ���ؼ�Ⱥ  
//            LocalCluster cluster = new LocalCluster();
              
            // �ύ���ˣ������˵����ֽ�word-count��
//            cluster.submitTopology("word-count", conf, builder.createTopology() );
            StormSubmitter.submitTopology("wc", conf, builder.createTopology());
//            Thread.sleep(10000);
        //  cluster.shutdown();  
        //}  
    }  
}
