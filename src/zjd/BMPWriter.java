package zjd;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImageOp;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by 金迪 on 2017/4/19.
 */
public class BMPWriter {
    BMPFileHeader bmpFileHeader;
    BMPInfoHeader bmpInfoHeader;
    BMPData bmpData;
//
    public BMPWriter(int width, int height, int bitCount) {
        int skip = 0;
        if(width*3%4!=0)
            skip = 4 - (width*3%4);

        int offset = 54;
//
        int size = width*height*3+skip*height+offset;

        bmpFileHeader = new BMPFileHeader(size, offset);
        bmpInfoHeader = new BMPInfoHeader(width, height, bitCount);
        bmpData = new BMPData(width, height);

        byte[] result = byteMerger(byteMerger(bmpFileHeader.getData(),bmpInfoHeader.getData()),bmpData.getData());
//        ByteArrayInputStream in = new ByteArrayInputStream(result);    //将b作为输入流；
//        BufferedImage image = ImageIO.read(in);
//        in.close();
//        return image;
    }

    /**
     * BMP结构文件头类BMPFileHeader
     * 固定14个字节，具体信息如下
     * 0-1格式头，为BM
     * 2-5文件大小，包括这14个字节
     * 6-9保留
     * 10-13从文件头到实际位图的偏移量
     */
    static class BMPFileHeader{
        private byte[] data = new byte[14];
        private int size;
        private int offset;

        public byte[] getData(){
            return this.data;
        }

        public int getSize(){
            return this.size;
        }

        public int getOffset(){
            return  this.offset;
        }

        BMPFileHeader(int size,int offset){
            this.size = size;
            this.offset = offset;

            data[0] = 'B';
            data[1] = 'M';

            int value = size;
            data[2] = (byte)value;
            value = value>>>8;
            data[3] = (byte)value;
            value = value>>>8;
            data[4] = (byte)value;
            value = value>>>8;
            data[5] = (byte)value;

            value = offset;
            data[10] = (byte)value;
            value = value>>>8;
            data[11] = (byte)value;
            value = value>>>8;
            data[12] = (byte)value;
            value = value>>>8;
            data[13] = (byte)value;
        }
    }

    /**
     * BMP信息文件头类BMPInfoHeader
     * 固定40个字节，其含义如下
     * 0-3指信息头的长度，为40（0x28）
     * 4-7指像图像宽度，单位为像素
     * 8-11指像素高度，单位为像素
     * 12-13为位面数，为1
     * 14-15为每个像素的位数，24真彩色为0x18
     * 16-19为是否压缩
     * 20-23为实际位图数据占用的字节数
     * 24-27为设备水平分辨率，单位是每米的像素个数
     * 28-31为设备垂直分辨率，单位是每米的像素个数
     * 32-35为本图像实际用到的颜色数
     * 36-39为本图中重要的颜色数，如果该值为0，则所有的颜色都为重要颜色
     */
    static class BMPInfoHeader{
        private byte[] data = new byte[40];
        private int width;
        private int height;
        private int bitCount;

        public byte[] getData() {
            return data;
        }

        public int getWidth() {
            return width;
        }

        public int getHeight() {
            return height;
        }

        public int getBitCount() {
            return bitCount;
        }

        public BMPInfoHeader(int width, int height, int bitCount){
            this.width = width;
            this.height = height;
            this.bitCount = bitCount;



            data[0] = 40;

            int value = width;
            data[4] = (byte) value;
            value = value>>>8;
            data[5] = (byte)value;
            value = value>>>8;
            data[6] = (byte)value;
            value = value>>>8;
            data[7] = (byte)value;

            value = height;
            data[8] = (byte) value;
            value = value>>>8;
            data[9] = (byte)value;
            value = value>>>8;
            data[10] = (byte)value;
            value = value>>>8;
            data[11] = (byte)value;

            data[12] = 1;

            data[14] = (byte) bitCount;

            value = width * height * 3;
            if(width%4 != 0)
                value += (width%4) * height;
            data[20] = (byte) value;
            value = value>>>8;
            data[21] = (byte)value;
            value = value>>>8;
            data[22] = (byte)value;
            value = value>>>8;
            data[23] = (byte)value;

            value = 3780;
            data[24] = data[28] = (byte) value;
            value = value>>>8;
            data[25] = data[29] = (byte)value;
            value = value>>>8;
            data[26] = data[30] = (byte)value;
            value = value>>>8;
            data[27] = data[31] = (byte)value;
        }
    }

    /**
     * 初始化数据数组
     */
    static class BMPData{
        private int width;
        private int height;
        private int skip;
        private byte[] data;

        public int getSkip() {
            return skip;
        }

        public byte[] getData() {
            return data;
        }

        public BMPData(int width,int height){
            this.width = width;
            this.height = height;

            skip = 0;
            if(width*3%4!=0)
                skip = 4 - (width*3%4);

            data = new byte[width*height*3+skip*height];
//            for (int i = 0;i<data.length;i++)
//                data[i] = (byte)0xFF;
        }
    }

    public BufferedImage init() throws IOException {
//        int skip = 0;
//        if(width*3%4!=0)
//            skip = 4 - (width*3%4);
//
//        int offset = 54;
//
//        int size = width*height*3+skip*height+offset;
//
//        BMPFileHeader bmpFileHeader = new BMPFileHeader(size, offset);
//        BMPInfoHeader bmpInfoHeader = new BMPInfoHeader(width, height, bitCount);
//        BMPData bmpData = new BMPData(width, height);

        byte[] result = byteMerger(byteMerger(bmpFileHeader.getData(),bmpInfoHeader.getData()),bmpData.getData());
        ByteArrayInputStream in = new ByteArrayInputStream(result);    //将b作为输入流；
        BufferedImage image = ImageIO.read(in);
        in.close();
        return image;
    }

    //byte数组合并
    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }

    //返回数据
    public byte[] returnData(){
        return bmpData.data;
    }

    //根据data
    public  BufferedImage returnImage(byte[] data) throws IOException {
        byte[] result = byteMerger(byteMerger(bmpFileHeader.getData(),bmpInfoHeader.getData()),data);
        ByteArrayInputStream in = new ByteArrayInputStream(result);    //将b作为输入流；
        BufferedImage image = ImageIO.read(in);
        in.close();
        return image;
    }

}
