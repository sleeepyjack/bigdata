package bigdata;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.LineRecordReader;
//import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class RelationRecordReader extends RecordReader<Text, TupleWritable> {
	
	private long start;
	private long pos;
	private long end;
	
	private Text key = new Text();
	private TupleWritable value = new TupleWritable();
	
	private LineReader reader;
	Path file;
	
	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public TupleWritable getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f,  (pos - start)/(float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		
		Configuration job = context.getConfiguration();
		
		start = split.getStart();
		end   = start + split.getLength();
		
		file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		boolean skipFirstLine = false;
		
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}
		
		reader = new LineReader(fileIn, job);
		
		if (skipFirstLine) {
			Text dummy = new Text();
			start += reader.readLine(dummy);
		}
		
		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(file.toString());
		
		int newSize = 0;
		
		while (pos < end) {
			Text line = new Text();
			newSize = reader.readLine(line);
			pos += newSize;
			if (!line.toString().startsWith("#")) {
				value.setAttributes(Arrays.asList(line.toString().split("\t")));
				
				if (newSize == 0) {
					break;
				}
			} 			
			
		}
		
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}
	
	
	
	
	
	
	
	
	

}
