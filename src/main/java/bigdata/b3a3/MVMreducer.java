package b3a3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MVMreducer extends Reducer<String, DoubleWritable, IntWritable, DoubleWritable> {
	public void reduce(String rawkey, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException {
		// Im Key ist die Position in der Zielmatrix hinterlegt
		// Einträge an der selben Position in der Zielmatrix müssen summiert werden
		String[] key = rawkey.split(",");
		int i = Integer.parseInt(key[0]);
		// j kann man hier ignorieren, da unsere Ergebnis Matrizen in der Aufgabenstellung ohnehin nur eine Spalte haben
		//int j = Integer.parseInt(key[1]);
		double sum = 0;
		// In der Iterable values mit dem Key (i,j) stehen alle SUmmanden in der Zielmatrix an der Poisition
		// Darüber summieren und das Ergebnis ist da
		// Kann auch für MxN Matrizen angepasst werden.
		for (DoubleWritable value : values) {
			sum+= value.get();
		}
		
		context.write(new IntWritable(i), new DoubleWritable(sum));
	}
	

}
