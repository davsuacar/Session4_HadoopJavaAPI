package david_suarez_1_improvement.david_suarez_1_improvement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NumbersTuple implements WritableComparable<NumbersTuple> {
	
	private DoubleWritable min;
	private DoubleWritable max;

	public NumbersTuple() {}

	public NumbersTuple(DoubleWritable min, DoubleWritable max) {
		this.min = min;
		this.max = max;
	}
	

	public void write(DataOutput out) throws IOException {
		min.write(out);
		max.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		min = new DoubleWritable(in.readDouble());
		max = new DoubleWritable(in.readDouble());
		
	}

	public DoubleWritable getmin() {
		return min;
	}



	public void setmin(DoubleWritable min) {
		this.min = min;
	}



	public DoubleWritable getmax() {
		return max;
	}



	public void setmax(DoubleWritable max) {
		this.max = max;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((min == null) ? 0 : min.hashCode());
		result = prime * result + ((max == null) ? 0 : max.hashCode());
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
		NumbersTuple other = (NumbersTuple) obj;
		if (min == null) {
			if (other.min != null)
				return false;
		} else if (!min.equals(other.min))
			return false;
		if (max == null) {
			if (other.max != null)
				return false;
		} else if (!max.equals(other.max))
			return false;
		return true;
	}


	  public int compareTo(NumbersTuple o) {
	    DoubleWritable thisMax = this.max;
	    DoubleWritable thisMin = this.min;
	    DoubleWritable thatMax = o.max;
	    DoubleWritable thatMin = o.min;
	    int maxCheck = thisMax.get() < thatMax.get() ? -1 : (thisMax==thatMax ? 0 : 1);
	    int minCheck = thisMin.get()<thatMin.get() ? -1 : (thisMin==thatMin ? 0 : 1);
	    return (maxCheck==0 ? minCheck : maxCheck);
	  }
	

	  @Override
	  public String toString() {
	    return Double.toString(min.get()) + " " + Double.toString(max.get());
	  }
	  
	  /** A Comparator optimized for NumbersTuple. */ 
	  public static class Comparator extends WritableComparator {
	    public Comparator() {
	      super(NumbersTuple.class);
	    }
	    
	    public int compare(byte[] b1, int start1, int length1,
	                       byte[] b2, int start2, int length2,
	                       byte[] b3, int start3, int length3,
	                       byte[] b4, int start4, int length4) {
	      int thisMax = readInt(b1, start1);
	      int thatMax = readInt(b2, start2);
	      int thisMin = readInt(b3, start3);
	      int thatMin = readInt(b4, start4);
	      
	      int maxCheck = thisMax < thatMax ? -1 : (thisMax==thatMax ? 0 : 1);
		  int minCheck = thisMin<thatMin ? -1 : (thisMin==thatMin ? 0 : 1);
		  return (maxCheck==0 ? minCheck : maxCheck);
	    }
	  }

	  static {                                
	    WritableComparator.define(NumbersTuple.class, new Comparator());
	  }
	  
	  
	  
	  

}
