// KeyValueTextInputtFormat implementieren (liest "0 a b c" als (0,"a b c") ein) --> muss noch gesplittet werden)
// Wenn aus dem input korrekt "DoubleListWritables" erzeugt werden der Form
// (K,V) = (Zeile, DoubleListWritables) müssen im Mapper die 3 Einträge mit den entsprechenden Doubles des
// Vektors multipliziert werden und im Reducer dann die Zeilen addiert werden.
// 
// | a b c |   |x|    | ax + by + cz |
// | d e f | * |y| =  | dx + ey + fz |
// | g h i |   |z|    | gx + hy + iz |
// 
// Entweder man mappt die Inputs direkt auf (K,V) = ( (i,j) , Zahlenwert) 
//-> dann weiß ich nicht wie ich den Reducer schreiben soll, sodass er parallel arbeiten kann
// Oder ich mappe auf (K,V) = (i, ZahlenWert). Dann weiß ich nicht wie ich sinnvoll den Multiplikationsvektor unterbringen soll
// Hadoop nervt, das scheiß DoubleListWritable macht auch keinen Sinn und ich hätte besser KI belegen sollen -.-

package b3a3;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MatrixVectorMult {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MatrixVectorMult");
		job.setJarByClass(MatrixVectorMult.class);
		job.setMapperClass(MVMmapper.class);
		job.setReducerClass(MVMreducer.class);
		
		job.setOutputKeyClass(DoubleListWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}