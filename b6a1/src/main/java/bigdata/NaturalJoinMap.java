//Create K,V-Pairs from input
package bigdata;

import java.io.IOException;
import java.util.List;

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
		List<String> temp;
		if (inputpath.toString().equals(inputpathR)) {
			temp = relation.getAttributes();
			temp.add(0,"R");
			TupleWritable value = new TupleWritable();
			value.setAttributes(temp);
			context.write(new Text(relation.getAttributes().get(indexR)), value);
		}
		else {
			temp = relation.getAttributes();
			temp.add(0,"L");
			TupleWritable value = new TupleWritable();
			value.setAttributes(temp);
			context.write(new Text(relation.getAttributes().get(indexL)), value);
		}
	}	
}