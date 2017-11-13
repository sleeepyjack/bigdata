// Ich weiß nicht wirklich wozu die Klasse dienen soll.
// Lediglich von Input zum KV-Paar würde das Sinn machen, macht aber nichts einfacher

package b3a3;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;



public class DoubleListWritable implements Writable {
	private ArrayList<Double> list = new ArrayList<Double>();
	public int getsize() {
		return list.size();
	}
	
	public double geti(int i) {
		return list.get(i);
	}
	public DoubleWritable getiwritable(int i) {
		return new DoubleWritable(this.geti(i));
	}
	public double getsum() {
		double sum = 0;
		int size = this.getsize();
		for (int i = 0; i < size; i++) {
			sum += list.get(i);
		}
		return sum;
	}
	public void readFields(DataInput in) throws IOException {
		int n = in.readInt();
		for (int i = 0; i < n; ++i) {
			this.list.add(in.readDouble());
		}
		
		
			
		}
	public void write(DataOutput out) throws IOException {
		int n = this.list.size();
		//out.writeInt(n);
		for (int i = 0; i < n; ++i) {
			out.writeDouble(list.get(i));
		}
		
		
	}


}