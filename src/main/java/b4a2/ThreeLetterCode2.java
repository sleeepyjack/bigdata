package b4a2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class ThreeLetterCode2 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser options = new GenericOptionsParser(conf, args);
    
    String remainingArguments[] = options.getRemainingArgs();
        
    Job job = Job.getInstance(conf, "ThreeLetterCode");

    job.setJarByClass(ThreeLetterCode.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    
    job.setMapperClass(ThreeLetterCodeMap.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setCombinerClass(ThreeLetterCodeReduce.class);
    job.setReducerClass(ThreeLetterCodeReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
    job.addCacheFile(new Path(remainingArguments[1]).toUri());
    FileOutputFormat.setOutputPath(job, new Path(remainingArguments[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
