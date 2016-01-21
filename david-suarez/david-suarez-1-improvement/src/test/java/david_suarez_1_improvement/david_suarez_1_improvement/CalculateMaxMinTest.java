package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for Histogram.
 */
public class CalculateMaxMinTest{

	@SuppressWarnings("rawtypes")
	private MapReduceDriver driver;
	  
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before 
	public void setUp() {
		driver = new MapReduceDriver(new CalculateMaxMinMapper(), new CalculateMaxMinReducer(), new CalculateMaxMinReducer());
  	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test() throws IOException {
		driver
		.withInput(new Text("23"), new Text("23"))
		.withInput(new Text("42"), new Text("42"))
		.withOutput(new IntWritable(1), (new NumbersTuple(new DoubleWritable(23.0), new DoubleWritable(42.0))))
		.runTest();		
	}	
}
