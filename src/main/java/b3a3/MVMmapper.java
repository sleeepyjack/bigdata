package b3a3;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;

public class MVMmapper extends Mapper<String,DoubleListWritable,String,DoubleWritable> {
	
	// Durch Magie kommt hier mit Hilfe des undokumentierten KeyValueInputTextFormat hoffentlich
	// ein DoubleListWritable an, das die Zeileneinträge zum entsprechenden Key enthält
	
	
	public void map(String key, DoubleListWritable value, 
			Context context) throws IOException,InterruptedException {
		int n = value.getsize();
		
		for(int i = 0; i< n; ++i) {
			for(int j = 0; j < 3; ++j) {
				context.write(i + "," + j, new DoubleWritable(value.geti(i)));
			}
		}
		if (key == "v") {
			for(int j = 0; j<n ; ++j) {
				context.write("v," + j, new DoubleWritable(value.geti(j)));
			}
		}
		
	}
}
