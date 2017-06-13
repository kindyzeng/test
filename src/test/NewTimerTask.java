package test;

import java.util.TimerTask;

public class NewTimerTask extends TimerTask{

	public int i;
	@Override
	public void run() {
		 OutputNumber();  
		
	}
	 private void OutputNumber() {  
	         
	        System.out.println("随机生成的数字为:"+i);  
	          
	    }
	public NewTimerTask() {
		super();
		this.i = 0;
	}  
}
