package david_suarez2.david_suarez2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FriendsReducer extends Reducer<Text, FriendsTuple, Text, Text>{
	
	public void reduce(Text key, Iterable<FriendsTuple> value, Context context)
			throws IOException, InterruptedException {
		
		List<Text> directRel = new ArrayList<Text>();
		List<Text> reverseRel = new ArrayList<Text>();
		
		for (FriendsTuple relation : value) {
			if(relation.getBool().toString().equals("true")){
				directRel.add(relation.getFriend());
			} else {
				reverseRel.add(relation.getFriend());
			}
		}
		
		for (int i = 0; i < reverseRel.size(); i++) {
			for (int j = 0; j < directRel.size(); j++) {
				context.write(reverseRel.get(i), directRel.get(j));
			}
		}
	}
}
