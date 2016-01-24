package david_suarez_2_improvement;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendsMapper extends Mapper<Text, Text, Text, FriendsTuple>{

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		System.out.println(key + "," + value);
		context.write(key, new FriendsTuple(new Text("true\n"), value));
		context.write(value, new FriendsTuple(new Text("false\n"), key));
	}
}
