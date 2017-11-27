package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NaturalJoinReduce extends Reducer<Text, TupleWritable, TupleWritable, Text> {

	String param;
	int indexL,indexR;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		indexL = Integer.parseInt(conf.get("joincol.left"));
		indexR = Integer.parseInt(conf.get("joincol.right"));
		
	}
	public void reduce(Text keyID, Iterable<TupleWritable> relations, Context context) throws IOException, InterruptedException {
	
	}
}