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
public class HistogramTest{

	private MapReduceDriver driver;
	  
	@SuppressWarnings("unchecked")
	@Before 
	public void setUp() {
		driver = new MapReduceDriver(new HistogramMapper(), new HistogramReducer(), new HistogramReducer());
		Configuration conf =driver.getConfiguration();
		conf.set("NumberOfBars", "2");
		conf.set("Max", "42");
		conf.set("Min", "23");
  	}
	
	@Test
	public void test() throws IOException {
		driver
		.withInput(new Text("23"), new Text("23"))
		.withInput(new Text("42"), new Text("42"))
		.withOutput(new IntWritable(0), new IntWritable(1))
		.withOutput(new IntWritable(1), new IntWritable(1))
		.runTest();		
	}	
}
