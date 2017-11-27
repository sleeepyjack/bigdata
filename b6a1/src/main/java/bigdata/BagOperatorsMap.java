//Create K,V-Pairs from input
package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.PENIS;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

public class BagOperatorsMap extends Mapper<Text, ListWritable, ListWritable, IntWritable> {
	
	publiv void setup(COntext context) {
		Configuration conf = context.getConfiguration();
		String param = conf.get("operation");
		String inputpath_R = der erste parameter beim aufruf..conf.
	}
	@Override
	public void map(Text inputpath, TupleWritable relation, Context context) throws IOException,InterruptedException {

		if (param.equals("bagunion")) {
			(context.write(relation, new IntWritable(1));
		}
		else (param.equals("bagintersection") || param.equals("bagdiffernce")) {
			if (inputpath.toString().equals(inputpath_R))) {
				context.write(relation,  new IntWritable(1));
			}
			else {
				context.write(relation, new IntWritable(0));
			}
		}
		
	}
}// headergedöns

//für bagunion:
//context.write(