package zjd;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by 金迪 on 2017/5/27.
 */
public class Test {
    public static void main(String[] args){
        System.out.println("开始啦");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date1 = new Date(System.currentTimeMillis());
        for (int i = 0;i<1000000000;i++){
            int l = i+1;
        }

        Date date2 = new Date(System.currentTimeMillis());


        long d = date2.getTime() - date1.getTime();

        String   str1   =   df.format(date1);
        String   str2   =   df.format(date2);
        System.out.println(date1.getTime()+" "+str1);
        System.out.println(date2.getTime()+" "+str2);
        System.out.print(d);
        if((date2.getTime()-date1.getTime())>1){
            System.out.println("Fuck");
        }
//        Runnable runnable = new Runnable() {
//            public void run() {
//                while (true) {
//                    System.out.println("Hello !!");
//                    try {
//                        Thread.sleep(5*1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        };
//        Thread thread = new Thread(runnable);
//        thread.start();
//
//        System.out.println("还没结束");
//
//        TimerTask task = new TimerTask() {
//            @Override
//            public void run() {
//                // task to run goes here
//                System.out.println("2!!!");
//            }
//        };
//        Timer timer = new Timer();
//        long delay = 10 * 1000;
//        long intevalPeriod = 1 * 1000;
//        // schedules the task to be run in an interval
//        timer.scheduleAtFixedRate(task, delay, intevalPeriod);
    }
}
