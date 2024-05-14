import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {
	private int positiveCount = 0;
    private int negativeCount = 0;
    private String overallSentiment = "Neutral"; // Default overall sentiment

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        for (Text value : values) {
            if (value.toString().equals("Positive")) {
                positiveCount++;
            } else if (value.toString().equals("Negative")) {
                negativeCount++;
            }
        }
     // Update overall sentiment based on cumulative counts
        if (positiveCount > negativeCount) {
            overallSentiment = "Positive";
        } else if (positiveCount < negativeCount) {
            overallSentiment = "Negative";
        } else {
            overallSentiment = "Neutral";
        }
        }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output cumulative counts and overall sentiment
        context.write(new Text("Overall Sentiment"), new Text("Positive Words: " + positiveCount + 
            ", Negative Words: " + negativeCount + ", Overall Sentiment: " + overallSentiment));
    }
}