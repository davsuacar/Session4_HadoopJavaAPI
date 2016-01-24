package david_suarez_2_improvement.david_suarez_2_improvement;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import david_suarez_2_improvement.FriendsMapper;
import david_suarez_2_improvement.FriendsReducer;

/**
 * Unit test for Friends.
 */
public class FriendsTest{
	
	@SuppressWarnings("rawtypes")
	private MapReduceDriver driver;
	  
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before 
	public void setUp() {
		driver = new MapReduceDriver(new FriendsMapper(), new FriendsReducer());
  	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test() throws IOException {
		driver
		.withInput(new Text("A"), new Text("B"))
		.withInput(new Text("C"), new Text("B"))
		.withOutput(new Text("C"), new Text("B"))
		.runTest();		
	}	

}
