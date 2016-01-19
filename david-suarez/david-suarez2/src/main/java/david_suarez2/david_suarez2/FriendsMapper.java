package david_suarez2.david_suarez2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendsMapper extends Mapper<Text, Text, Text, FriendsTuple>{

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
	
		context.write(key, new FriendsTuple(new Text("true"), value));
		context.write(value, new FriendsTuple(new Text("false"), key));
	}

}
