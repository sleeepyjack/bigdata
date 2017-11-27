//Create K,V-Pairs from input
package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

public class BagOperatorsMap extends Mapper<Text, TupleWritable, TupleWritable, IntWritable> {
	
	String inputpathR;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		inputpathR = conf.get("RelationR");
	}
	@Override
	public void map(Text inputpath, TupleWritable relation, Context context) throws IOException,InterruptedException {
		String inputpathR = "abc";		//der_erste_parameter_beim_aufruf;
		if (inputpath.toString().equals(inputpathR)) {
			context.write(relation,  new IntWritable(1));
		}
		else {
			context.write(relation, new IntWritable(0));
		}
	}	
}