package test;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class testCount  implements CombinerAggregator<Long>
{
	  public Long init(TridentTuple tuple)
	  {
	     
//		  String a = tuple.getString(0);
//		  System.out.print(a);
		  if(tuple.getString(0)=="YES")
		  {
		  return Long.valueOf(1L);
		  }
		  else {
			  return Long.valueOf(0L);
		}
	  }

	  public Long combine(Long val1, Long val2)
	  {
	    return Long.valueOf(val1.longValue() + val2.longValue());
	  }

	  public Long zero()
	  {
	    return Long.valueOf(0L);
	  }
	}
