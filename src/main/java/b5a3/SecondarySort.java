package b5a3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser options = new GenericOptionsParser(conf, args);
    String remainingArguments[] = options.getRemainingArgs();
    
    Job job = Job.getInstance(conf, "simple statistics");
    job.setJarByClass(SecondarySort.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setSortComparatorClass(SecondarySortComparator.class);
    //Mapper sollte erste Zeile als Key und 
    job.setMapperClass(SecondarySortMap.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setReducerClass(SecondarySortReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
    FileOutputFormat.setOutputPath(job, new Path(remainingArguments[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
