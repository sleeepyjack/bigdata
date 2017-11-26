package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class TupleWritable implements WritableComparable<TupleWritable> {

	private List<String> attributes;
	
	public List<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}

	public void readFields(DataInput in) throws IOException {
		String s = in.readLine();
		attributes = Arrays.asList(s.split("\t"));		
	}

	public void write(DataOutput out) throws IOException {
		String s = String.join("\t", attributes);
		out.writeChars(s);
	}

	public int compareTo(TupleWritable other) {
		if (this.equals(other)) return 0; //shortcut if equal
		
		//check for equality attribute-wise
		Iterator<String> iThis  = this.attributes.iterator();
		Iterator<String> iOther = other.attributes.iterator();
		
		while (iThis.hasNext() && iOther.hasNext()) {
			int comp = iThis.next().compareTo(iOther.next());
			if (comp != 0) return comp;
		}
		
		//in case the tuples have different lengths (should never happen)
		return (this.attributes.size() < other.attributes.size()) ? -1 : 1;		
	}
	
	@Override
	public boolean equals(Object o) {
		TupleWritable other = (TupleWritable) o;
		//return this.attributes.equals(other.attributes);
		return (this.hashCode() == other.hashCode());
	}
	
	@Override
	public int hashCode() {
		//return hashCode of attribute list
		return this.attributes.hashCode();
	}

}
