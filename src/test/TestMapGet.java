package test;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public  class TestMapGet  extends BaseQueryFunction<ReadOnlyMapState, Object>{
	public List<Object> batchRetrieve(ReadOnlyMapState map, List<TridentTuple> keys)
	  {
//		TridentTuple a = (TridentTuple) new Values("");
//		System.out.println(keys);
//		System.out.println("\n");
//	   for(int i=0;i<keys.size();i++)
//	   {
//		  keys.get(i).remove(0);
//	   }
		

		return map.multiGet(keys);
		 
	  }

	  public void execute(TridentTuple tuple, Object result, TridentCollector collector)
	  {
	    collector.emit(new Values(new Object[] { result }));
	    
	  }

}
