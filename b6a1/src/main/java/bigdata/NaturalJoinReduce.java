package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		List<List<String>> relationsR = new ArrayList<List<String>>();
		List<List<String>> relationsL = new ArrayList<List<String>>();
		for(TupleWritable relation : relations) {
			List<String> tuple = relation.getAttributes();
			if (tuple.get(0).equals("R")) {
				//Remove indicator entry
				tuple.remove(0);
				//Add the relations to the list
				relationsR.add(tuple);				
			}
			if (tuple.get(0).equals("L")) {
				tuple.remove(0);
				relationsL.add(tuple);				
			}			
		}
		//Join every L-entry with every R-entry
		for (List<String> relationR : relationsR) {
			for (List<String> relationL : relationsL) {
				List<String> tempadd = new ArrayList<String>();
				//Create temporary list holding the relations, remove the "join index in the right side list"
				tempadd.addAll(relationR);
				tempadd.remove(indexR);
				relationL.addAll(relationR);
				TupleWritable result = new TupleWritable();
				result.setAttributes(relationL);
				context.write(result, new Text(""));
			}
		}
		
		
	
	}
}