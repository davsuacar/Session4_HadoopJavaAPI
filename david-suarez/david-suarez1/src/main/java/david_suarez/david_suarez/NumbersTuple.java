package david_suarez.david_suarez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

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
		int cmp = min.compareTo(o.min);
		if (cmp != 0) {
		return cmp;
		} else
		return max.compareTo(o.getmax());
	}
	

	  @Override
	  public String toString() {
	    return Double.toString(min.get()) + " " + Double.toString(max.get());
	  }

}
