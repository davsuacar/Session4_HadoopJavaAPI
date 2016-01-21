package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class CalculateMaxMinReducer extends Reducer<IntWritable, NumbersTuple, IntWritable, NumbersTuple> {
	@Override
	public void reduce(IntWritable key, Iterable<NumbersTuple> values, Context context)
			throws IOException, InterruptedException {
		

		
		
		Double min = Double.MAX_VALUE;
		Double max = Double.MIN_VALUE;
		
		for (NumbersTuple maxmin : values) {
			min = Math.min(min, maxmin.getmin().get());
			max = Math.max(max, maxmin.getmax().get());
		}
		
		context.write(key, new NumbersTuple(new DoubleWritable(min), new DoubleWritable(max)));
	}
}
