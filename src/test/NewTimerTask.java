package test;

import java.util.TimerTask;

public class NewTimerTask extends TimerTask{

	public int i;
	@Override
	public void run() {
		 OutputNumber();  
		
	}
	 private void OutputNumber() {  
	         
	        System.out.println("������ɵ�����Ϊ:"+i);  
	          
	    }
	public NewTimerTask() {
		super();
		this.i = 0;
	}  
}
