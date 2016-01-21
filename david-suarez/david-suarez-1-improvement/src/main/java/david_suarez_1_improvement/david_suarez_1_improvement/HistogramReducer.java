package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	
	
	public void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		
		Integer total = 0;	
		for (IntWritable count : value) {
			total += count.get();
		}
		context.write(key, new IntWritable(total));
	}
}
