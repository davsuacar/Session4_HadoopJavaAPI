package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistogramMapper extends Mapper<Text, Text, IntWritable, IntWritable>{

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Integer n = Integer.parseInt(conf.get("NumberOfBars"));
		Double max = new Double(conf.get("Max"));
		Double min = new Double(conf.get("Min"));
		
		// Receive the number again
		Double number = new Double(key.toString());
		
		Double bar = Math.floor((number - min)/((max - min)/n));
		
		if(number.equals(max)){
			context.write(new IntWritable(n - 1), new IntWritable(1));
		} else {
			context.write(new IntWritable(new Integer(bar.intValue())), new IntWritable(1));
		}
	}
	
	
}
