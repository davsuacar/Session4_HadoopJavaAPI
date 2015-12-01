package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistogramMapper extends Mapper<DoubleWritable, NumbersTuple, IntWritable, IntWritable>{

	@Override
	public void map(DoubleWritable key, NumbersTuple value, Context context) throws IOException,
			InterruptedException {
		
		Double n = null; //TAKE VALUE FROM INPUT ARGUMENTS;
		Double number = key.get();
		Double min = value.getmin().get();
		Double max = value.getmax().get();
		
		Double bar = Math.floor((number - min)/((min - max)/n));
		
		if(number.equals(max)){
			context.write(n - 1, new Integer(1));
		}
		
		context.write(new IntWritable(new Integer(bar.intValue())), new IntWritable(1));
	}
}
