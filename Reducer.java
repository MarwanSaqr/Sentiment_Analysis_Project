import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private int positiveCount = 0;
    private int negativeCount = 0;

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
    }
    
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
    // Calculate overall sentiment
    String overallSentiment;
    if (positiveCount > negativeCount) {
        overallSentiment = "Positive";
    } else if (positiveCount == negativeCount) {
        overallSentiment = "Neutral";
    } else {
        overallSentiment = "Negative";
    }
    // Write the overall sentiment along with counts to the context
    outputValue.set("Positive Words: " + positiveCount + ", Negative Words: " + negativeCount + ", Overall Sentiment: " + overallSentiment);
    context.write(outputKey, outputValue);
}
}
