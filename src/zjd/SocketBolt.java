package zjd;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.grid.Entity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by 金迪 on 2017/5/27.
 */
public class SocketBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Entity entity = (Entity) tuple.getValue(0);

        //将entity写入txt
        BufferedWriter out = null;
        try {
            FileWriter fileWriter = new FileWriter("usr/local/software/1.txt",true);
            out = new BufferedWriter(fileWriter);
            out.write("\n" + entity.getID() + "" + entity.getX() + " " + entity.getY() + "\n");
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
