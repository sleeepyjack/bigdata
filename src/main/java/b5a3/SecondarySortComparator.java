package b5a3;

import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class SecondarySortComparator extends WritableComparator {
    public void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	int col = conf.getInt("sorter.orderby", 0); //column selector (default 1)
    }
	
	public int compare(Text text1, Text text2) {
		return text1.toString().compareTo(text2.toString());
	}

}
