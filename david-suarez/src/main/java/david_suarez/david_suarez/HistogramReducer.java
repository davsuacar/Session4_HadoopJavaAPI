package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HistogramReducer extends Reducer<IntWritable, Iterable<IntWritable>, IntWritable, IntWritable>{
	
	
	public void reducer(IntWritable key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		Integer total = 0;
		for (IntWritable count : value) {
			total += count.get();
		}
		context.write(key, new IntWritable(total));
	}
}
