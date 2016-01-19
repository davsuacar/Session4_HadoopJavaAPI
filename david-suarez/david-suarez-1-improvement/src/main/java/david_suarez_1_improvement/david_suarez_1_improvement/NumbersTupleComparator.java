package david_suarez_1_improvement.david_suarez_1_improvement;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NumbersTupleComparator extends WritableComparator{
	
	public NumbersTupleComparator(){
        super(NumbersTuple.class);
    }
    @Override
    public int compare(WritableComparable o,WritableComparable o2){
        NumbersTuple m = (NumbersTuple)o;
        NumbersTuple m2 = (NumbersTuple)o2;
        int cmp = m.getmax().compareTo(m2.getmax());
        if (cmp != 0) {
		return cmp;
		} else
		return m.getmin().compareTo(m2.getmin());
    }
}
