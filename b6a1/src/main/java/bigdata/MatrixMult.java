package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMult {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    GenericOptionsParser options = new GenericOptionsParser(conf, args);
	    
	    String remainingArguments[] = options.getRemainingArgs();
	        
	    Job job = Job.getInstance(conf, "MatrixMult");
	    //Set first Relationname as Relation "R"

	    job.setJarByClass(MatrixMult.class);
	    
	    job.setMapperClass(MatrixMultMap.class);
	    //Input f�r Mapper: (Pfad,ListWritable)
	    //Ausgabe des Mappers sind Tupel (
	    job.setMapOutputKeyClass(DoubleListWritable.class);
	    job.setMapOutputValueClass(DoubleListWritable.class);
	    // (l,k),(X,i,j,v) & (k,m),(X,i,j,v) (X Indikator für Matrix A oder B)
	    
	    job.setReducerClass(MatrixMultRed.class);
	    job.setOutputKeyClass(DoubleListWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    // (i,j),v (Position in Ausgabe und entsprechender Wert)
	    
	    FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
	    FileInputFormat.addInputPath(job, new Path(remainingArguments[1]));
	    FileOutputFormat.setOutputPath(job, new Path(remainingArguments[4]));
	    conf.set("l",remainingArguments[2]);
	    conf.set("m",remainingArguments[3]);
	    conf.set("MatrixA",remainingArguments[0]);
	    conf.set("MatrixB",remainingArguments[1]);

	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}