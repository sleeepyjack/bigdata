package b3a1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimpleStatistics {

public static class Map extends Mapper<Object, Text, Text, FloatWritable>{

    private Text   k = new Text();
    private FloatWritable v = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	if (!value.toString().startsWith("#")) {
    		StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
    		int i = 0;
    		if (itr.hasMoreTokens()) {
				itr.nextToken(); //ommit first column
			}
        	while (itr.hasMoreTokens()) {
        		k.set(Integer.toString(++i));
        		v.set(Float.parseFloat(itr.nextToken()));
        		System.out.println(k + " " + v);
        		context.write(k, v);
        	}
		}
    }
  }

  public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
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
      
      context.write(new Text("Sum_" + key.toString()), new FloatWritable(sum));
      context.write(new Text("Max_" + key.toString()), new FloatWritable(max));
      context.write(new Text("Avg_" + key.toString()), new FloatWritable(avg));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "simple statistics");
    job.setJarByClass(SimpleStatistics.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
