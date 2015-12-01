package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateMaxMinMapper extends Mapper<Text, NullWritable, IntWritable, NumbersTuple>{

	@Override
	public void map(Text key, NullWritable value, Context context) throws IOException,
			InterruptedException {
		
		DoubleWritable limit = new DoubleWritable(new Double(key.toString())); 
		
		context.write(new IntWritable(1), new NumbersTuple(limit, limit));
	}
}
