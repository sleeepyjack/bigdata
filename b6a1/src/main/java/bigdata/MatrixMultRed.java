package bigdata;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class MatrixMultRed extends Reducer<DoubleListWritable, DoubleListWritable, DoubleListWritable, DoubleWritable>{
	int l,m;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		l = Integer.parseInt(conf.get("l").toString());
		m = Integer.parseInt(conf.get("m").toString());
	}
	public void reduce(DoubleListWritable key, Iterable<DoubleListWritable> values, Context context) throws IOException, InterruptedException {
		ArrayList<ArrayList<Double>> matrixA = new ArrayList<ArrayList<Double>>();
		ArrayList<ArrayList<Double>> matrixB = new ArrayList<ArrayList<Double>>();
		ArrayList<Double> temp = new ArrayList<Double>();
		double result = 0;
		
		for(DoubleListWritable value : values) {
			if (value.geti(0) == 0) {
				temp.add(value.geti(1));
				temp.add(value.geti(2));
				matrixA.add(temp);
				temp.clear();
			}
			if (value.geti(0) == 1.0) {
				temp.add(value.geti(1));
				temp.add(value.geti(2));
				matrixB.add(temp);
				temp.clear();
			}
		}
		//Überprüft in n^2 auf "matches" für den Index J
		for (ArrayList<Double> entryA : matrixA) {
			for (ArrayList<Double> entryB : matrixB) {
				if (entryA.get(0) == entryB.get(0)) {
					result+=entryA.get(1)*entryB.get(1);
					break;
				}
			}
		}
		ArrayList<Double> output = new ArrayList<Double>();
		output.add(key.geti(0));
		output.add(key.geti(1));
		context.write(new DoubleListWritable(output),new DoubleWritable(result));
		
		
	}

}
