package test;
import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
@SuppressWarnings("serial")
public class StringTest implements ReducerAggregator<String>{

	@Override
	public String init() {
		// TODO Auto-generated method stub
		
		return "";
	}

	@Override
	public String reduce(String arg0, TridentTuple arg1) {
		// TODO Auto-generated method stub
		return arg0+"zy ";
	}

	

	
   
}
