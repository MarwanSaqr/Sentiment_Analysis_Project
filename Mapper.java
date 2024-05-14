import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
	 private Text outputKey = new Text();
	    private Text outputValue = new Text();
	    private HashSet<String> positiveWordList = new HashSet<>();
	    private HashSet<String> negativeWordList = new HashSet<>();
	    

	    @Override
	    
	    protected void setup(Context context) throws IOException, InterruptedException {
	        // Load positive word list
	        loadSentimentList("/home/cloudera/workspace/positive.txt", positiveWordList);
	        
	        // Load negative word list
	        loadSentimentList("/home/cloudera/workspace/negative.txt", negativeWordList);

	       
	    }

	    private void loadSentimentList(String filename, HashSet<String> set) throws IOException {
	        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
	            String line;
	            while ((line = br.readLine()) != null) {
	                // Split the line by commas and add each word to the set
	                String[] words = line.split(",");
	                for (String word : words) {
	                    set.add(word.trim().toLowerCase()); // Add trimmed lowercase word to set
	                }
	            }
	        }
	    }
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	        
	        String[] tokens = value.toString().split("\\s+");
	        for (String token : tokens) {
	            String sentiment = "Neutral"; // Default sentiment

	            // Remove commas and convert token to lowercase for case-insensitive matching
	            String cleanedToken = token.replaceAll(",", "").toLowerCase();

	            // Check if token is a word
	            if (positiveWordList.contains(cleanedToken)) {
	                sentiment = "Positive";  } 
			else if (negativeWordList.contains(cleanedToken)) {
	                    sentiment = "Negative";
	            
	            outputKey.set(token);
	            outputValue.set(sentiment);
	            context.write(outputKey, outputValue);
	        }
	    }
	    }