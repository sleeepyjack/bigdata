package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;

public class MatrixMultMap extends Mapper<DoubleListWritable, DoubleListWritable, DoubleListWritable, DoubleListWritable>{
	int l,m;
	String matrixA, matrixB;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		l = Integer.parseInt(conf.get("l").toString());
		m = Integer.parseInt(conf.get("m").toString());
		matrixA = conf.get("MatrixA");
		matrixB = conf.get("MatrixB");
	}
	public void map(Text origin, DoubleListWritable input, Context context) throws IOException,InterruptedException {
		ArrayList<Double> key = new ArrayList<Double>();
		ArrayList<Double> value = new ArrayList<Double>();
		
		if (origin.toString().equals(matrixA)) {
			for (int x = 0; x <=l; x++) {
				key.add(input.geti(0)); //i
				key.add((double) x); //k
				value.add(0.0);
				value.add(input.geti(1));
				value.add(input.geti(2));
				context.write(new DoubleListWritable(key),new DoubleListWritable(value));
				key.clear();
				value.clear();
			}
		}
		if (origin.toString().equals(matrixB)) {
			for (int x = 0; x <=m; x++) {
				key.add((double) x); //k
				key.add(input.geti(1)); //j
				value.add(1.0);
				value.add(input.geti(0));
				value.add(input.geti(2));
				context.write(new DoubleListWritable(key),new DoubleListWritable(value));
				key.clear();
				value.clear();
			}
		}
	}
}
