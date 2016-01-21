package david_suarez_1_improvement.david_suarez_1_improvement;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HistogramDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.printf("Usage: Histogram <input dir> <output dir> <output first job> <number bars>\n");
			System.exit(-1);
		}
		
		// Read from hdfs
		Configuration confi = new Configuration();
	    Path path = new Path("/home/davidsuarez/out/part-r-00000");
	    FileSystem hdfs = FileSystem.get(confi);
	    FSDataInputStream inputStream = hdfs.open(path);
	    String[] line = inputStream.readLine().split(" |\t");
	    inputStream.close();

		// Take max and min
		String min = line[1];
		String max = line[2];
		
		// Setting parameters
		Configuration conf = new Configuration();
		conf.set("NumberOfBars", args[2]);
		
		// Set max and min to config
		conf.set("Max", max);
		conf.set("Min", min);
		
		Job job = new Job(conf, "Histogram");
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(HistogramMapper.class);
		job.setCombinerClass(HistogramReducer.class);
		job.setReducerClass(HistogramReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
