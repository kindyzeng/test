package index;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class TestPartition implements Partitioner {

	  public TestPartition (VerifiableProperties props) {
		  
	    }
	@Override
	public int partition(Object key, int a_numPartitions) {
	        return Integer.parseInt((String) key)  % a_numPartitions;
	}

}
