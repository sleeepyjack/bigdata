package bigdata;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleStatistics {

public static class Map extends Mapper<Object, Text, NullWritable, FloatWritable>{
    private FloatWritable v = new FloatWritable();
    private int	col;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	col = conf.getInt("stats.col", 1); //column selector (default 1)
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	if (!value.toString().startsWith("#")) { //if input is no comment
    		String[] tokens = value.toString().split("\t");
    		
    		if (tokens.length > col) { //sanity check
    			v.set(Float.parseFloat(tokens[col]));
    			context.write(NullWritable.get(), v); //only write value
			}
		}
    }
  }

  public static class Reduce extends Reducer<NullWritable, FloatWritable, Text, FloatWritable> {

    public void reduce(NullWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      float num = 0;
      float max = Float.MIN_VALUE;
      float avg;
      
      for (FloatWritable val : values) {
        sum += val.get();
        max  = (val.get() > max) ? val.get() : max;
        num++;
      }
      
      avg = sum/num;
      
      context.write(new Text("Max"), new FloatWritable(max));
      context.write(new Text("Sum"), new FloatWritable(sum));
      context.write(new Text("Avg"), new FloatWritable(avg));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser options = new GenericOptionsParser(conf, args);
    String remainingArguments[] = options.getRemainingArgs();
    
    Job job = Job.getInstance(conf, "simple statistics");
    job.setJarByClass(SimpleStatistics.class);
    
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(FloatWritable.class);
    
    //job.setCombinerClass(Reduce.class); //use reducer as combiner
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
    FileOutputFormat.setOutputPath(job, new Path(remainingArguments[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
