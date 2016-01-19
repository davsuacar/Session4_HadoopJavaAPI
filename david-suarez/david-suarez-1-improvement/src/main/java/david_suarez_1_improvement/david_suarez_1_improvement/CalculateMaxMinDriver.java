package david_suarez_1_improvement.david_suarez_1_improvement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalculateMaxMinDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.printf("Usage: MaxMin <input dir> <output dir>\n");
			System.exit(-1);
		}
				
		Job job = new Job();
		job.setJobName("MaxMin");
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CalculateMaxMinMapper.class);
		job.setReducerClass(CalculateMaxMinReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NumbersTuple.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NumbersTuple.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
