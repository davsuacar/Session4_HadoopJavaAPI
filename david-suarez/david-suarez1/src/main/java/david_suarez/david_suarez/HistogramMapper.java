package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class HistogramMapper extends Mapper<DoubleWritable, NumbersTuple, IntWritable, IntWritable>{

	private static Integer n;

	public void configure(JobConf job) {
	     n = Integer.parseInt(job.get("NumberOfBars"));
	}
	@Override
	public void map(DoubleWritable key, NumbersTuple value, Context context) throws IOException,
			InterruptedException {
		
		Double number = key.get();
		Double min = value.getmin().get();
		Double max = value.getmax().get();
		
		Double bar = Math.floor((number - min)/((min - max)/n));
		
		if(number.equals(max)){
			context.write(new IntWritable(n - 1), new IntWritable(1));
		} else {
			context.write(new IntWritable(new Integer(bar.intValue())), new IntWritable(1));
		}
	}
	
	
}