//Convert single char input to 3-letter output
package bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import java.net.URI;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;

public class ThreeLetterCodeReduce extends Reducer<Text, Text, Text, Text> {
	static enum CountersEnum { AMINO_ACIDS }


    public void reduce(Text proteinIn, Iterable<Text> iterableInput, Context context) throws IOException, InterruptedException {
    	
    	HashMap<String,String> OneToThree = new HashMap<String,String>();
		URI URIpath = context.getCacheFiles()[0];
		Path inputPath = new Path(URIpath.getPath());
		String inputName = inputPath.getName().toString();
		BufferedReader inputBfr = new BufferedReader(new FileReader(inputName));
		String nextLine;
		String oneLetterCode ="";
		String threeLetterCode = "";
		while ((nextLine = inputBfr.readLine()) != null) {
			StringTokenizer itr = new StringTokenizer(nextLine);
			oneLetterCode = itr.nextToken();
			threeLetterCode = itr.nextToken();			
			OneToThree.put(oneLetterCode, threeLetterCode);
			
		}
		inputBfr.close();
		Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.AMINO_ACIDS.toString());
    	
    	
    	//Der Output erscheint gar nicht erst im hadoop output ...
    	//context.write(new Text("REDUCE TEST"), new Text(inputString.toString()));
	    String inputString;
		for (Text input : iterableInput) {
	    	inputString = input.toString();
    		int size = inputString.length();
	    	String output = "";
	    	String oneLetter = "";
	    	for (int i = 0; i < size; i++ ) {
	    		oneLetter = Character.toString(inputString.charAt(i));
	    		output += (OneToThree.get(oneLetter));
	    		counter.increment(1);

	    	}
	    	context.write(new Text(proteinIn.toString()), new Text(output));
	    }
	    	
    	//Der Output sollte eigtl. funktionieren ...

    	
    	
    	//context.write(new Text("1"), new Text(OneToThree.get("A")));
    	//context.write(new Text("REDUCE"), new Text("2"));
    	

  }
}
