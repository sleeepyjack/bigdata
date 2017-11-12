//Create K,V-Pairs from input
package b4a2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreeLetterCodeMap extends Mapper<Text, Text, Text, Text> {
	
	@Override
	public void map(Text protein, Text sequence, Context context) throws IOException,InterruptedException {

		//context.write(new Text("a"), new Text("b"));
		
		context.write(new Text(protein.toString()), sequence);
	}
}

