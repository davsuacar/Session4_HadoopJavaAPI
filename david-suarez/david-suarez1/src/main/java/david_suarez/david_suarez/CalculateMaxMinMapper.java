package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateMaxMinMapper extends Mapper<Text, Text, IntWritable, NumbersTuple>{

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		
		DoubleWritable limit = new DoubleWritable(new Double(key.toString()));
		
		NumbersTuple valueToSend = new NumbersTuple(limit, limit);
		
		context.write(new IntWritable(1), valueToSend);
	}
}
