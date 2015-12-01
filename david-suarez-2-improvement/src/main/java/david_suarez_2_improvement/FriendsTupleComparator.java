package david_suarez_2_improvement;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FriendsTupleComparator extends WritableComparator{
	public FriendsTupleComparator(){
        super(FriendsTuple.class);
    }
    @Override
    public int compare(WritableComparable o,WritableComparable o2){
    	FriendsTuple m = (FriendsTuple)o;
    	FriendsTuple m2 = (FriendsTuple)o2;
        int cmp = m.getBool().compareTo(m2.getBool());
        if (cmp != 0) {
		return cmp;
		} else
		return m.getFriend().compareTo(m2.getFriend());
    }
}
