package bigdata;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;

public class MVMmapper extends Mapper<String,DoubleListWritable,String,DoubleWritable> {
	
	// Durch Magie kommt hier mit Hilfe des undokumentierten KeyValueInputTextFormat hoffentlich
	// ein DoubleListWritable an, das die Zeileneinträge zum entsprechenden Key enthält
	// Daraus werden KV-PAare der Form Key = i,j) Value = Double erzeugt.
	// i,j gibt die Position in der Ergebnismatrix an (Prinzipiell 
	
	
	public void map(String key, DoubleListWritable value, 
			Context context) throws IOException,InterruptedException {
		if (key == "v") {
			int n = value.getsize();
			for (int i = 0; i < n; ++i) {
				for(int j = 0; j < 3; ++j) {
					context.write(i + ","+j, value.getiwritable(i));
				}
				
			}
		}
		else {
			int n = value.getsize();
			for (int i = 0; i < n; ++i) {
				for (int j = 0 ; j < 3; ++j) {
					context.write(i + ","+j, value.getiwritable(i));
				}
			}
		}
		
	}
}
