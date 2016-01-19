package david_suarez2.david_suarez2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FriendsTuple implements WritableComparable<FriendsTuple>{
	
	private Text bool;
	private Text friend;

	public FriendsTuple(Text bool, Text friend) {
		super();
		this.bool = bool;
		this.friend = friend;
	}

	public Text getBool() {
		return bool;
	}

	public void setBool(Text bool) {
		this.bool = bool;
	}

	public Text getFriend() {
		return friend;
	}

	public void setFriend(Text friend) {
		this.friend = friend;
	}

	public void write(DataOutput out) throws IOException {
		bool.write(out);
		friend.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		bool.readFields(in);
		friend.readFields(in);
		
	}

	public int compareTo(FriendsTuple o) {
		int cmp = bool.compareTo(o.bool);
		if (cmp != 0) {
		return cmp;
		} else
		return friend.compareTo(o.getFriend());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bool == null) ? 0 : bool.hashCode());
		result = prime * result + ((friend == null) ? 0 : friend.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FriendsTuple other = (FriendsTuple) obj;
		if (bool == null) {
			if (other.bool != null)
				return false;
		} else if (!bool.equals(other.bool))
			return false;
		if (friend == null) {
			if (other.friend != null)
				return false;
		} else if (!friend.equals(other.friend))
			return false;
		return true;
	}
	
	

}
