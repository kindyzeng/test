package zjd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import storm.kafka.*;
import test.DistributeSingleHGridIndexBolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by 金迪 on 2017/6/1.
 */
public class V2RTopology {private final BrokerHosts brokerHosts;
    public V2RTopology() {
        brokerHosts = new ZkHosts("202.121.180.3:2181,202.121.180.4:2181,202.121.180.5:2181,202.121.180.6:2181,202.121.180.7:2181");
    }

    public StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"biye","","biye");
        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        topologyBuilder.setSpout("DataSpout",new KafkaSpout(spoutConfig),3);
        topologyBuilder.setBolt("CalBolt",new CalBolt(),3).shuffleGrouping("DataSpout");
        topologyBuilder.setBolt("GraBolt",new GraBolt(),3).fieldsGrouping("CalBolt",new Fields("key"));

//        topologyBuilder.setBolt("HGridIndex",new DistributeSingleHGridIndexBolt(),5).setMaxTaskParallelism(50).shuffleGrouping("DataSpout");

//        topologyBuilder.setSpout("UpdateData",new KafkaSpout(spoutConfig),3);
//        topologyBuilder.setBolt("Print",new UpdateDataTopology.PrinterBolt(),3).shuffleGrouping("UpdateData");
//        topologyBuilder.setBolt("HGridIndex",  new DistributeSingleHGridIndexBolt(),5).setMaxTaskParallelism(50).fieldsGrouping("Print", new Fields("ID"));
        return topologyBuilder.createTopology();
    }

    public static class GraBolt extends BaseBasicBolt{
        private double x0,y0,x1,y1,d0,d;
        private int z = 2;
        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            this.x0 = Double.parseDouble(stormConf.get("x0").toString());
            this.x1 = Double.parseDouble(stormConf.get("x1").toString());
            this.y0 = Double.parseDouble(stormConf.get("y0").toString());
            this.y1 = Double.parseDouble(stormConf.get("y1").toString());

            //定义d
            if ((x1 - x0) > (y0 - y1))
                d0 = x1 - x0;
            else
                d0 = y0 - y1;
            d = d0 / Math.pow(2, z); //定义栅格间距
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String key = tuple.getString(0);
            String wkt = tuple.getString(1);
            String c = key.toString().split(",")[1];
            String r = key.toString().split(",")[2];
            String z = key.toString().split(",")[0];

            //每一个格网的px0,py0,px1,py1,pd
            double px0 = x0 + Double.parseDouble(c) * d;
            double px1 = x0 + Double.parseDouble(c) + d;
            double py0 = y0 - Double.parseDouble(r) * d;
            double py1 = y0 - Double.parseDouble(r) * d - d;
            double pdx = d / 256;
            double pdy = d / 256;

            String dirPath = "/usr/local/" + c + "/" + r;
            File dir = new File(dirPath);
            if (!dir.exists())
                dir.mkdirs();

            File file = new File("/usr/local/1.txt");
            if(!file.exists()){
                file.setWritable(true,false);
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            BMPWriter bmpWriter = new BMPWriter(256,256,24);
            byte[] result = bmpWriter.returnData();
//
//
//            String a = "";
//            for(Text re:values){
//                String wkt = re.toString().split(":")[0];
//                String pList = re.toString().split(":")[1];
//
//                BufferedWriter out = null;
//                try {
//                    FileWriter fileWriter = new FileWriter("/usr/local/2.txt",true);
//                    out = new BufferedWriter(fileWriter);
//                    out.write(key.toString()+"\n"+re.toString()+"\n");
//                    out.flush(); // 把缓存区内容压入文件
//                    out.close(); // 最后记得关闭文件
//                    //Geometry geometry = GeometryEngine.geometryFromWkt("POLYGON ((0 0,1 0,1 1,0 0))", 0, Geometry.Type.Polygon);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//                Geometry per = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
//                Envelope eper =  new Envelope();
//                per.queryEnvelope(eper);
//
//                int pc1 = returnGrid(eper.getXMin(),eper.getYMax(),pdx,pdy,px0,py0)[0];
//                int pr1 = returnGrid(eper.getXMin(),eper.getYMax(),pdx,pdy,px0,py0)[1];
//                int pc2 = returnGrid(eper.getXMax(),eper.getYMin(),pdx,pdy,px0,py0)[0];
//                int pr2 = returnGrid(eper.getXMax(),eper.getYMin(),pdx,pdy,px0,py0)[1];
//
//                String[] polygons = pList.split(";");
//
//                for (String polygon:polygons) {
//                    //遍历格网
//                    for (int dr = pr1; dr < pr2; dr++) {
//                        for (int dc = pc1; dc < pc2; dc++) {
//                            double x = dc * pdx + px0 + pdx / 2.0;
//                            double y = py0 - dr * pdy - pdy / 2.0;
//                            //判断点在不在此多边形内部，如果是，格网上色
//                            if (IsPtInRegion(x, y, returnPList(polygon))) {
//                                result[(256 - dr - 1) * 256 * 3 + dc * 3] = (byte) 0xFF;
//                                result[(256 - dr - 1) * 256 * 3 + dc * 3 + 1] = (byte) 0xFF;
//                                result[(256 - dr - 1) * 256 * 3 + dc * 3 + 2] = (byte) 0xFF;
//                            }
//                        }
//                    }
//                }
//
//                //保存图片
//                BufferedImage image = bmpWriter.returnImage(result);
//                ImageIO.write(image, "bmp", new File(dirPath + "/1" +".bmp"));
//
//                a+=values.toString()+" ok!! ";
//            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static class CalBolt extends BaseBasicBolt{
        private double x0,y0,x1,y1,d0,d;
        private int z = 2;
        private SpatialReference sr = SpatialReference.create(4326);

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            this.x0 = Double.parseDouble(stormConf.get("x0").toString());
            this.x1 = Double.parseDouble(stormConf.get("x1").toString());
            this.y0 = Double.parseDouble(stormConf.get("y0").toString());
            this.y1 = Double.parseDouble(stormConf.get("y1").toString());

            //定义d
            if ((x1 - x0) > (y0 - y1))
                d0 = x1 - x0;
            else
                d0 = y0 - y1;
            d = d0 / Math.pow(2, z); //定义栅格间距
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String line = tuple.getString(0);

            //测试内容
            BufferedWriter out = null;
            try {
                FileWriter fileWriter = new FileWriter("/usr/local/software/1.txt",true);
                out = new BufferedWriter(fileWriter);
                out.write("x0:"+x0+" y1:"+y1+" "+line+"\r\n");
                out.flush(); // 把缓存区内容压入文件
                out.close(); // 最后记得关闭文件
            } catch (IOException e) {
                e.printStackTrace();
            }

            //将wkt格式的数据转化为geometry
            Geometry geometry = GeometryEngine.geometryFromWkt(line,0, Geometry.Type.Polygon);

            //求每个Geometry的外包矩形框
            Envelope envelope = new Envelope();
            geometry.queryEnvelope(envelope);

            //定义矢量经过的栅格网号
            int c1 = (int) ((envelope.getXMin() - x0) / d);
            int r1 = (int) ((y0 - (envelope.getYMax())) / d);
            int c2 = (int) Math.ceil((envelope.getXMax() - x0) / d) - 1;
            int r2 = (int) Math.ceil((y0 - (envelope.getYMin())) / d) - 1;

                //遍历矢量经过的每一个栅格
                for (int r = r1; r <= r2; r++) {
                    for (int c = c1; c <= c2; c++) {
                        //计算每一个格网与矢量的相交
                        Envelope grid = new Envelope(x0 + c * d, y0 - r * d - d, x0 + c * d + d, y0 - r * d);
                        Geometry intersect = GeometryEngine.intersect(geometry, grid, sr);

                        //如果矢量与格网的相交不为空，把相应的数据输出
                        if (!intersect.isEmpty()) {
                            String wkt = GeometryEngine.geometryToWkt(intersect, 0);
                            basicOutputCollector.emit(new Values(z + "," + c + "," + r,wkt));
//                            if (wkt.startsWith("POLYGON")) {
//                                wkt = wkt.replace("POLYGON ((", "").replace("))", "");
////                                context.write(new Text(z + "," + c + "," + r), new Text(getOutput(wkt)));
//                            }
//                            else if (wkt.startsWith("MULTIPOLYGON")) {
//                                wkt = wkt.replace("MULTIPOLYGON (((", "").replace(")))", "").replace(")), ((",")),((");
//                                String[] polygons = wkt.split("\\)\\),\\(\\(|\\)\\), \\(\\(");
//                                for (String polygon : polygons) {
//                                    basicOutputCollector.emit(new Values(z + "," + c + "," + r,wkt));
//                                }
//                            }
                        }
                    }
                }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("key","value"));
        }
    }

        //根据d,r,c返回输出字符串
        private static String getOutput(String polygon) {
            String output = "";
            String[] paths = polygon.split("\\),\\(|\\), \\(");
            for (String path : paths) {
                path = path.replace(")","").replace("(","");
                String points[] = path.split(",");
                for (String str : points) {
                    String xy[] = str.trim().split(" ");
                    output += xy[0] + " " + xy[1] + ",";
                }
                output += ";";
            }
            return output;
        }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, IOException {
        File file = new File("/usr/local/software/1.txt");
        if(!file.exists()){
            file.setWritable(true,false);
            file.createNewFile();
        }

        Config conf = new Config();
        conf.setNumWorkers(66);
        conf.setMaxTaskParallelism(99);
        conf.put(Config.NIMBUS_HOST, "192.168.1.100");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("192.168.1.101","192.168.1.102"));

        V2RTopology topology = new V2RTopology();
        StormTopology stormTopology = topology.buildTopology();

        if (args != null && args.length > 0) {
            //获取x0,x1,y0,y1
            conf.put("x0",args[1]);
            conf.put("x1",args[2]);
            conf.put("y0",args[3]);
            conf.put("y1",args[4]);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, stormTopology);
        }
        else {
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, stormTopology);
            //			Thread.sleep(100000);
            //			cluster.shutdown();
        }

    }
}
