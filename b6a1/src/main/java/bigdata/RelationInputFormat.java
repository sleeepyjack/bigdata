package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class RelationInputFormat extends FileInputFormat<Text, TupleWritable> {

	@Override
	public RecordReader<Text, TupleWritable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
