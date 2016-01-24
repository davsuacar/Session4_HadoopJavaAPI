package david_suarez_2_improvement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class FriendsTuple implements WritableComparable<FriendsTuple>{
	
	private Text bool;
	private Text friend;

	public FriendsTuple() {
	}
	
	public FriendsTuple(Text bool, Text friend) {
		this.bool = bool;
		this.friend = friend;
	}
	
	public void write(DataOutput out) throws IOException {
		bool.write(out);
		friend.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		bool = new Text(in.readLine().trim());
		System.out.println("Bool " + bool);
		friend = new Text(in.readLine().trim());
		System.out.println("Friend " + friend);
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
	
	  @Override
	  public String toString() {
	    return bool.toString() + "\t" + friend.toString();
	  }
	  
	  /** A Comparator optimized for NumbersTuple. */ 
	  public static class Comparator extends WritableComparator {
	    public Comparator() {
	      super(FriendsTuple.class);
	    }
	    
	    public int compare(byte[] b1, int start1, int length1,
	                       byte[] b2, int start2, int length2,
	                       byte[] b3, int start3, int length3,
	                       byte[] b4, int start4, int length4) {
	    	try {
	    		int thisBool = readVInt(b1, start1);
				int thatBool = readVInt(b2, start2);
				int thisFriend = readVInt(b3, start3);
				int thatFriend = readVInt(b4, start4);
			  
				int boolCheck = thisBool < thatBool ? -1 : (thisBool==thatBool ? 0 : 1);
				int friendCheck = thisFriend<thatFriend ? -1 : (thisFriend==thatFriend ? 0 : 1);
				return (boolCheck==0 ? friendCheck : friendCheck);
	    	} catch (IOException e) {
	    		throw new RuntimeException(e);
	    	}
	    }
	  }

	  static {                                
	    WritableComparator.define(FriendsTuple.class, new Comparator());
	  }
	

}
