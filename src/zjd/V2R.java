//package zjd;
//
//import java.awt.Color;
//import java.awt.Graphics2D;
//import java.awt.geom.GeneralPath;
//import java.awt.image.BufferedImage;
//import java.io.*;
//import java.lang.reflect.Field;
//import java.util.DoubleSummaryStatistics;
//import java.util.Properties;
//
//import javax.imageio.ImageIO;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//
//import com.esri.core.geometry.Envelope;
//import com.esri.core.geometry.Geometry;
//import com.esri.core.geometry.GeometryEngine;
//import com.esri.core.geometry.SpatialReference;
//
///**
// * Created by 金迪 on 2017/5/9.
// */
//public class V2R {
//
//
//    public static class V2RMapper extends Mapper<LongWritable, Text, Text, Text>{
//        private int z = 1; //定义切图的个数指数，横纵均为pow(2,k)个切图
//        private double x0,y0,x1,y1,d0; //定义整个地图的坐标范围(),总距离
//        private SpatialReference sr = SpatialReference.create(4326);
//
//        //根据d,r,c返回输出字符串
//        private static String getOutput(String polygon) {
//            String output = "";
//            String[] paths = polygon.split("\\),\\(|\\), \\(");
//            for (String path : paths) {
//                path = path.replace(")","").replace("(","");
//                String points[] = path.split(",");
//                for (String str : points) {
//                    String xy[] = str.trim().split(" ");
//                    output += xy[0] + " " + xy[1] + ",";
//                }
//                output += ";";
//            }
//            return output;
//        }
//        @Override
//        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            x0 = Double.parseDouble(context.getConfiguration().get("x0"));
//            x1 = Double.parseDouble(context.getConfiguration().get("x1"));
//            y0 = Double.parseDouble(context.getConfiguration().get("y0"));
//            y1 = Double.parseDouble(context.getConfiguration().get("y1"));
//        }
//
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//                String arr = value.toString();
////                System.out.println(arr);
//            if (arr.startsWith("POLYGON")) {
////                BufferedWriter out = null;
////                try {
////                    FileWriter fileWriter = new FileWriter("/usr/local/1.txt",true);
////                    out = new BufferedWriter(fileWriter);
////                    out.write(arr+"\n");
////                    out.flush(); // 把缓存区内容压入文件
////                    out.close(); // 最后记得关闭文件
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
//
//                //将wkt格式的矢量图转化为geometry格式
//                Geometry geometry =  GeometryEngine.geometryFromWkt(arr, 0, Geometry.Type.Polygon);
//
//                Envelope envelope = new Envelope();
//                geometry.queryEnvelope(envelope);
//
//                if ((x1 - x0) > (y0 - y1))
//                    d0 = x1 - x0;
//                else
//                    d0 = y0 - y1;
//                double d = d0 / Math.pow(2, z); //定义栅格间距
//
//                //定义矢量经过的栅格网号
//                int c1 = (int) ((envelope.getXMin() - x0) / d);
//                int r1 = (int) ((y0 - (envelope.getYMax())) / d);
//                int c2 = (int) Math.ceil((envelope.getXMax() - x0) / d) - 1;
//                int r2 = (int) Math.ceil((y0 - (envelope.getYMin())) / d) - 1;
//
//                //遍历矢量经过的每一个栅格
//                for (int r = r1; r <= r2; r++) {
//                    for (int c = c1; c <= c2; c++) {
//                        //计算每一个格网与矢量的相交
//                        Envelope grid = new Envelope(x0 + c * d, y0 - r * d - d, x0 + c * d + d, y0 - r * d);
//                        Geometry intersect = GeometryEngine.intersect(geometry, grid, sr);
//
//                        if (!intersect.isEmpty()) {
//                            String wkt1 = GeometryEngine.geometryToWkt(intersect, 0);
//                            if (wkt1.startsWith("POLYGON")) {
//                                String wkt = wkt1.replace("POLYGON ((", "").replace("))", "");
//                                context.write(new Text(z + "," + c + "," + r), new Text(getOutput(wkt)));
//                            } else if (wkt1.startsWith("MULTIPOLYGON")) {
//                                String wkt = wkt1.replace("MULTIPOLYGON (((", "").replace(")))", "").replace(")), ((",")),((");
//                                String[] polygons = wkt1.split("\\)\\),\\(\\(|\\)\\), \\(\\(");
//                                for (String polygon : polygons) {
//                                    context.write(new Text(z + "," + c + "," + r), new Text(wkt1+":"+getOutput(wkt)));
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//
//        }
//
//
//    }
//
//    public static class V2RReducer extends Reducer<Text,Text,Text,Text>{
//        private double x0,y0,x1,y1,d0; //定义整个地图的坐标范围(),总距离
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            x0 = Double.parseDouble(context.getConfiguration().get("x0"));
//            x1 = Double.parseDouble(context.getConfiguration().get("x1"));
//            y0 = Double.parseDouble(context.getConfiguration().get("y0"));
//            y1 = Double.parseDouble(context.getConfiguration().get("y1"));
//            if ((x1 - x0) > (y0 - y1))
//                d0 = x1 - x0;
//            else
//                d0 = y0 - y1;
//        }
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String c = key.toString().split(",")[1];
//            String r = key.toString().split(",")[2];
//            String z = key.toString().split(",")[0];
//
//            double d = d0 / Math.pow(2, Double.parseDouble(z)); //定义栅格间距
//            double px0 = x0 + Double.parseDouble(c) * d;
//            double px1 = x0 + Double.parseDouble(c) + d;
//            double py0 = y0 - Double.parseDouble(r) * d;
//            double py1 = y0 - Double.parseDouble(r) * d - d;
//            double pdx = d / 256;
//            double pdy = d / 256;
//
//            String dirPath = "/usr/local/" + c + "/" + r;
//            File dir = new File(dirPath);
//            if (!dir.exists())
//                dir.mkdirs();
////
////            File file = new File("/usr/local/1.txt");
////            if(!file.exists()){
////                file.setWritable(true,false);
////                file.createNewFile();
////            }
//
//            BMPWriter bmpWriter = new BMPWriter(256,256,24);
//            byte[] result = bmpWriter.returnData();
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
//
//            context.write(key,new Text(a));
//        }
//
//        //根据输出字符串返回Plist
//        private static double[] returnPList(String wkt){
//            String[] points = wkt.split(",");
//            int i = points.length;
//            double[] re = new double[2*i];
//            for (int j = 0;j < i;j++){
//                String point = points[j].trim();
//                re[2*j] = Double.parseDouble(point.split(" ")[0]);
//                re[2*j+1] = Double.parseDouble(point.split(" ")[1]);
//            }
//            return re;
//        }
//
//        //计算点所在的网格号
//        public static int[] returnGrid(double x,double y,double dx,double dy,double x0,double y0){
//            int[] re = new int[2];
//            re[0] = (int)((x - x0) / dx);
//            if(re[0] == 256)
//                re[0] = 255;
//            re[1] = (int)((y0 - y) / dy);
//            if(re[1] == 256)
//                re[1] = 255;
//            return re;
//        }
//
//        //射线法判断点是否在多边形内部，多边形的边用double数组来储存{x1,y1,x2,y2....}
//        public static boolean IsPtInRegion(double x, double y, double[] pList)
//        {
//            int nCross = 0;
//            int num = pList.length/2;
//            //定义遍历多边形每条边的前后端点
//            double x1,y1,x2,y2;
//            for (int i = 0; i < num; i++)
//            {
//                x1 = pList[2 * i]; y1 = pList[2 * i + 1];
//                x2 = pList[(2 * (i + 1)) % (2 * num)];y2 = pList[((2 * (i + 1)) + 1 ) % (2 * num)];    //最后一个点与第一个点连线
//                //POINT p1 = pList[i];
//                //POINT p2 = pList[(i + 1) % num];// 最后一个点与第一个点连线
//                if (y1 == y2)
//                    continue;
//                if (y < Math.min(y1, y2))
//                    continue;
//
//                if (y >= Math.max(y1, y2))
//                    continue;
//
//                // 求交点的x坐标
//                double x0 = (double)(y - y1) * (double)(x2 - x1) / (double)(y2 - y1) + x1;
//
//                // 只统计p1p2与p向右射线的交点
//                if (x0 > x)
//                {
//                    nCross++;
//                }
//            }
//            // 交点为偶数，点在多边形之外
//            return (nCross % 2 == 1);
//        }
//
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        File file = new File("/usr/local/1.txt");
//        if(!file.exists()){
//            file.setWritable(true,false);
//            file.createNewFile();
//        }
//
//        file = new File("/usr/local/2.txt");
//        if(!file.exists()){
//            file.setWritable(true,false);
//            file.createNewFile();
//        }
//
//
//        Configuration conf = new Configuration();
////        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
////            if (otherArgs.length != 8) {
////                System.out.println("hdfsInput hdfsOutput numReduceTasks z baseDir colorPropIndex isMerge geoType");
////                System.exit(2);
////            }
////        String hdfsInputPath = otherArgs[0];
////        String[] hdfsInputDir=hdfsInputPath.split(",");
////        String hdfsOutputPath = otherArgs[1];
//        //int reduceTaskNumber = Integer.parseInt(otherArgs[2]);
//        conf.set("x0", args[2]);
//        conf.set("x1", args[3]);
//        conf.set("y1", args[4]);
//        conf.set("y0", args[5]);
//        Job job = new Job(conf);
//        job.setJarByClass(V2R.class);
//        job.setMapperClass(V2RMapper.class);
//        job.setReducerClass(V2RReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileSystem fs = FileSystem.get(conf);
//        Path outputDir = new Path(args[1]);
//        if(fs.exists(outputDir)) {
//            fs.delete(outputDir,true);
//            System.out.println("the outputDir is exist,but it has been deleteed!");
//        }
//        FileOutputFormat.setOutputPath(job, outputDir);
//        System.exit(job.waitForCompletion(true)? 0: 1);
//
////            for(int i=0;i<hdfsInputDir.length;i++){
////                FileInputFormat.addInputPath(job, new Path(hdfsInputDir[i]));
////            }
//    }
//
//    /*
//    public static class TileMapper extends Mapper<Object, Text, Text, Text> {
//        private double org = 20037508.3427892;
//        private SpatialReference sr = SpatialReference.create(4326);
//        private String z_value;
//        private int colorPropIndex;
//        private Properties symbol = new Properties();
//        private int zmin = 1, zmax = 2;
//        private int geoType = 0;
//
//        private String getOutput(double a, int r, int c, String polygon) {
//            String output = "";
//            String[] paths = polygon.split("\\),\\(");
//            for (String path : paths) {
//                String points[] = path.split(",");
//                long tempx = -1;
//                long tempy = -1;
//                for (String str : points) {
//                    String xy[] = str.trim().split(" ");
//                    long px = Math.round((Double.valueOf(xy[0]) + org - c * a) / a * 256);
//                    long py = 256 - Math.round((Double.valueOf(xy[1]) + org - r * a) / a * 256);
//                    if (!(px == tempx && py == tempy))
//                        output += px + " " + py + ",";
//                    tempx = px;
//                    tempy = py;
//                }
//                output += ";";
//            }
//            return output;
//        }
//
//        @Override
//        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            z_value = context.getConfiguration().get("z", "1");
//            colorPropIndex = Integer.parseInt(context.getConfiguration().get("colorPropIndex", "1"));
//            geoType = Integer.parseInt(context.getConfiguration().get("geoType", "0"));
//            String z_range[] = z_value.split("-");
//            if (z_range.length == 1) {
//                zmin = Integer.parseInt(z_range[0]);
//                zmax = zmin + 1;
//            } else {
//                zmin = Integer.parseInt(z_range[0]);
//                zmax = Integer.parseInt(z_range[1]) + 1;
//            }
//
//            InputStream in = V2R.class.getResourceAsStream("color.properties");
//            symbol.load(in);
//        }
//
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String arr[] = value.toString().split("\t");
////			String color = "#000000";
//
//            //确定颜色的代码
//            String color = "#AECFAE";
//            String str = (String) symbol.get(arr[colorPropIndex]);
//            if(str != null && !str.equals("")){
//                color = str;
//            }
//
//            Geometry geo = null;
//            try {
//                if(geoType == 0)
//                    geo = GeometryEngine.geometryFromWkt(arr[arr.length - 1], 0,
//                            Geometry.Type.Polygon);
//                else if(geoType == 1)
//                    geo = GeometryEngine.geometryFromWkt(arr[arr.length - 1], 0,
//                            Geometry.Type.Polyline);
//                else if(geoType == 2)
//                    geo = GeometryEngine.geometryFromWkt(arr[arr.length - 1], 0,
//                            Geometry.Type.Point);
//            } catch (Exception e) {
//                return;
//            }
//            Envelope env = new Envelope();
//            geo.queryEnvelope(env);
//            for (int z = zmin; z < zmax; z++) {
//                double a = 2 * org / Math.pow(2, z);
//                int cmin = (int) ((env.getXMin() + org) / a);
//                int cmax = (int) Math.ceil((env.getXMax() + org) / a) - 1;
//                int rmin = (int) ((env.getYMin() + org) / a);
//                int rmax = (int) Math.ceil((env.getYMax() + org) / a) - 1;
//                if (cmin < 0 || cmax > Math.pow(2, z) - 1 || rmin < 0 || rmax > Math.pow(2, z) - 1) {
//                    return;
//                }
//                try {
//                    for (int r = rmin; r <= rmax; r++) {
//                        for (int c = cmin; c <= cmax; c++) {
//                            Envelope grid = new Envelope(-org + c * a, -org + r * a, -org + c * a + a, -org + r * a + a);
//                            Geometry intersect = GeometryEngine.intersect(geo, grid, sr);
//                            if (!intersect.isEmpty()) {
//                                String wkt = GeometryEngine.geometryToWkt(intersect, 0);
//                                if(geoType == 0){
//                                    if (wkt.startsWith("POLYGON")) {
//                                        wkt = wkt.replace("POLYGON ((", "").replace("))", "");
//                                        context.write(new Text(z + "," + c + "," + r), new Text(color + ":" + getOutput(a, r, c, wkt)));
//                                    } else if (wkt.startsWith("MULTIPOLYGON")) {
//                                        wkt = wkt.replace("MULTIPOLYGON (((", "").replace(")))", "");
//                                        String[] polygons = wkt.split("\\)\\),\\(\\(");
//                                        for (String polygon : polygons) {
//                                            context.write(new Text(z + "," + c + "," + r), new Text(color + ":" + getOutput(a, r, c, polygon)));
//                                        }
//                                    }
//                                }else if(geoType == 1){
//                                    if (wkt.startsWith("LINESTRING")) {
//                                        wkt = wkt.replace("LINESTRING (", "").replace(")", "");
//                                        context.write(new Text(z + "," + c + "," + r), new Text(color + ":" + getOutput(a, r, c, wkt)));
//                                    } else if (wkt.startsWith("MULTILINESTRING")) {
//                                        wkt = wkt.replace("MULTILINESTRING ((", "").replace("))", "");
//                                        String[] polylines = wkt.split("\\),\\(");
//                                        for (String polyline : polylines) {
//                                            context.write(new Text(z + "," + c + "," + r), new Text(color + ":" + getOutput(a, r, c, polyline)));
//                                        }
//                                    }
//                                }else if(geoType == 2){
//                                    wkt = wkt.replace("POINT (", "").replace(")", "");
//                                    context.write(new Text(z + "," + c + "," + r), new Text(color + ":" + getOutput(a, r, c, wkt)));
//                                }
//                            }
//                        }
//                    }
//                } catch (Exception e) {
//                    return;
//                }
//            }
//        }
//    }
//    */
//
//    /*
//    public static class TileReducer extends Reducer<Text, Text, Text, Text> {
//        private String baseDir = "";
//        private boolean isMerge;
//        private int geoType = 0;
//
//        @Override
//        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            baseDir = context.getConfiguration().get("baseDir", "/output/vectorallLayer");
//            isMerge = context.getConfiguration().get("isMerge", "0").equals("1");
//            geoType = Integer.parseInt(context.getConfiguration().get("geoType", "0"));
//        }
//
//        public void reduce(Text key, Iterable<Text> values, Context context) {
//            try {
//                BufferedImage img = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB);
//                String[] temp = key.toString().split(",");
//                File dir = new File(baseDir + "/" + temp[0] + "/" + temp[1]);
//                if (!dir.exists())
//                    dir.mkdirs();
//                File output = new File(baseDir + "/" + temp[0] + "/" + temp[1] + "/" + temp[2] + ".png");
//                if (isMerge && output.exists()) {
//                    img = ImageIO.read(output);
//                }
//                Graphics2D g2d = img.createGraphics();
//                for (Text value : values) {
//                    String[] arr = value.toString().split(":");
//                    Color c = Color.decode(arr[0]);
//                    g2d.setColor(c);
//                    if(geoType != 2){
//                        String[] paths = arr[1].split(";");
//                        GeneralPath gp = new GeneralPath(GeneralPath.WIND_EVEN_ODD);
//                        for (String path : paths) {
//                            GeneralPath sub_gp = new GeneralPath(GeneralPath.WIND_EVEN_ODD);
//                            String points[] = path.split(",");
//                            if(points.length == 0){
//                                continue;
//                            }
//                            String[] xy0 = points[0].split(" ");
//                            sub_gp.moveTo(Double.parseDouble(xy0[0]), Double.parseDouble(xy0[1]));
//                            for(int i=1;i<points.length;i++){
//                                String xy[] = points[i].split(" ");
//                                sub_gp.lineTo(Double.parseDouble(xy[0]), Double.parseDouble(xy[1]));
//                            }
//                            if (geoType == 0) {
//                                sub_gp.closePath();
//                            }
//                            gp.append(sub_gp, false);
//                        }
//                        if(geoType == 0)
//                            g2d.fill(gp);
//                        else if(geoType == 1)
//                            g2d.draw(gp);
//                    }else{
//                        String xy[] = arr[1].replace(",", "").replace(";", "").split(" ");
//                        g2d.fillOval(Integer.parseInt(xy[0]), Integer.parseInt(xy[1]), 5, 5);
//                    }
//                }
//                g2d.dispose();
//                ImageIO.write(img, "png", output);
//            } catch (NumberFormatException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//    */
//
//    /*
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 8) {
//            System.out.println("hdfsInput hdfsOutput numReduceTasks z baseDir colorPropIndex isMerge geoType");
//            System.exit(2);
//        }
//        String hdfsInputPath = otherArgs[0];
//        String[] hdfsInputDir=hdfsInputPath.split(",");
//        String hdfsOutputPath = otherArgs[1];
//        int reduceTaskNumber = Integer.parseInt(otherArgs[2]);
//        conf.set("z", otherArgs[3]);
//        conf.set("baseDir", otherArgs[4]);
//        conf.set("colorPropIndex", otherArgs[5]);
//        conf.set("isMerge", otherArgs[6]);
//        conf.set("geoType", otherArgs[7]);
//        Job job = new Job(conf);
//        job.setJarByClass(V2R.class);
//        job.setMapperClass(TileMapper.class);
//        job.setReducerClass(TileReducer.class);
//        job.setNumReduceTasks(reduceTaskNumber);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        for(int i=0;i<hdfsInputDir.length;i++){
//            FileInputFormat.addInputPath(job, new Path(hdfsInputDir[i]));
//        }
//
//
//        try{
//            FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));
//            boolean isSuccessful=job.waitForCompletion(true);
//            FileSystem hdfs = FileSystem.get(conf);
//            if(hdfs.exists(new Path(hdfsOutputPath))){
//                hdfs.delete(new Path(hdfsOutputPath),true);
//                System.exit( isSuccessful? 0 : 1);
//            }
//        }catch(Exception ex){
//            FileSystem hdfs = FileSystem.get(conf);
//            if(hdfs.exists(new Path(hdfsOutputPath))){
//                hdfs.delete(new Path(hdfsOutputPath),true);
//                System.exit(1);
//            }
//        }
//    }
//    */
//}
