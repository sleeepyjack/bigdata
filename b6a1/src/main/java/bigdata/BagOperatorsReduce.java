package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.PENIS;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

public class BagOperatorsReduce extends Mapper<TupleWritable, IntWritable, TupleWritable, IntWritable> {
	
	publiv void setup(COntext context) {
		Configuration conf = context.getConfiguration();
		String param = conf.get("operation");
		String inputpath_R = der erste parameter beim aufruf..conf.
	}
	public void reduce(TupleWritable relation, Iterable<IntWritable> counts) {
		int result=0;
		int num=0;
		if (param.equals("bagunion") {
			for (IntWritable count : counts) {
				result+=count.get();
			}
			context.write(relation, new IntWritable(result));
		}
		if (param.equals("bagintersection")) {
			for (IntWritable count: counts) {
				result+=count.get();
				num++;
			}
			//taucht die relation in a auf: 1
			//taucht die relation in b auf: 0
			//Anzahl in a == result
			//Anzahl in b == num - result
			context.write(relation, new IntWritable(Math.min(result,num - result)));
		}
		if (param.equals("bagdifference")) {
			for (IntWritable count: counts) {
				if (count.get() == 1)
					result++;
				else
					result--;
			}
			context.write(relation, new IntWritable(Math.max(0, result)));
			
		}
	}