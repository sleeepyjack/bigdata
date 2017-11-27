package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class RelationInputFormat extends FileInputFormat<Text, TupleWritable> {

	@Override
	public RecordReader<Text, TupleWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return (RecordReader<Text, TupleWritable>) new RelationRecordReader();
	}

}
