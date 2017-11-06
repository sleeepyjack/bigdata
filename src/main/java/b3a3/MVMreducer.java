package b3a3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MVMreducer extends Reducer<String, DoubleWritable, IntWritable, DoubleWritable> {
	public void reduce(String rawkey, DoubleWritable values, Context context) throws IOException,InterruptedException {
		double sum = 0;
		String[] key = rawkey.split(",");
		if (key[0] != "v") {
			// 
		}
		
		for (DoubleWritable value : values) {
			sum += value.get();			
		}
		context.write(key, new DoubleWritable(sum));
	}
	

}
