package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class BagOperators {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser options = new GenericOptionsParser(conf, args);
    
    String remainingArguments[] = options.getRemainingArgs();
        
    Job job = Job.getInstance(conf, "BagOperators");

    job.setJarByClass(BagOperators.class);
    job.setInputFormatClass(RelationInputFormat.class);
    
    job.setMapperClass(BagOperatorsMap.class);
    //Input für Mapper: (Pfad,ListWritable)
    //Ausgabe des Mappers sind Tupel (
    job.setMapOutputKeyClass(ListWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setReducerClass(BagOperatorsReduce.class);
    job.setOutputKeyClass(ListWritable.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
    job.addCacheFile(new Path(remainingArguments[1]).toUri());
    FileOutputFormat.setOutputPath(job, new Path(remainingArguments[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
