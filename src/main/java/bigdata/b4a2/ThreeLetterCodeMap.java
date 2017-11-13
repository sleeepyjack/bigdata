//Create K,V-Pairs from input
package b4a2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

public class ThreeLetterCodeMap extends Mapper<Text, Text, Text, Text> {
	
	static enum CountersEnum { PROTEINS }
	@Override
	public void map(Text protein, Text sequence, Context context) throws IOException,InterruptedException {

		//context.write(new Text("a"), new Text("b"));
		Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.PROTEINS.toString());
		counter.increment(1);
		context.write(new Text(protein.toString()), sequence);
	}
}

