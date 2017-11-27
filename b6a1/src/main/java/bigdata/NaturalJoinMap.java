//Create K,V-Pairs from input
package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class NaturalJoinMap extends Mapper<Text, TupleWritable, Text, TupleWritable> {
	int indexL, indexR;
	String inputpathR;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		indexL = Integer.parseInt(conf.get("joincol.left"));
		indexR = Integer.parseInt(conf.get("joincol.right"));
		inputpathR = conf.get("RelationR");		
	}
	@Override
	public void map(Text inputpath, TupleWritable relation, Context context) throws IOException,InterruptedException {
		//String inputpathR = "abc";		//der_erste_parameter_beim_aufruf;
		if (inputpath.toString().equals(inputpathR)) {
			context.write(new Text(relation.getAttributes().get(indexR)), relation);
		}
		else {
			context.write(new Text(relation.getAttributes().get(indexL)), relation);
		}
	}	
}