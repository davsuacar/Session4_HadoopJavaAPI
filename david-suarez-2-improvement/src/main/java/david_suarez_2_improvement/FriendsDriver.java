package david_suarez_2_improvement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FriendsDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.printf("Usage: Friends <input dir> <output dir>\n");
			System.exit(-1);
		}
				
		Job job = new Job();
		job.setJobName("Friends");
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FriendsTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
