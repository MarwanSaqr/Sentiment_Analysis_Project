import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private HashSet<String> positiveWordList = new HashSet<>();
    private HashSet<String> negativeWordList = new HashSet<>();
    private HashSet<String> positiveEmojiList = new HashSet<>();
    private HashSet<String> negativeEmojiList = new HashSet<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load positive word list
        loadSentimentList("/home/cloudera/workspace/positive.txt", positiveWordList);

        // Load negative word list
        loadSentimentList("/home/cloudera/workspace/negative.txt", negativeWordList);
        // Load positive emoji list
        loadSentimentList("/home/cloudera/workspace/emoji_positive.txt", positiveEmojiList);

        // Load negative emoji list
        loadSentimentList("/home/cloudera/workspace/emoji_negative.txt", negativeEmojiList);
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
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            String sentiment = "Neutral"; // Default sentiment
            // Remove non-alphanumeric characters and convert token to lowercase for case-insensitive matching
            String cleanedToken = token.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

            // Check if token is a word
            if (positiveWordList.contains(cleanedToken)) {
                sentiment = "Positive";
            } else if (negativeWordList.contains(cleanedToken)) {
                sentiment = "Negative";
            }
         // Check if token is an emoji
            if (isEmoji(token)) {
                if (positiveEmojiList.contains(token)) {
                    sentiment = "Positive";
                } else if (negativeEmojiList.contains(token)) {
                    sentiment = "Negative";
                }
            }
            outputKey.set(token);
            outputValue.set(sentiment);
            context.write(outputKey, outputValue);
        }
    }
    public static boolean isEmoji(String token) {
        // Define a pattern to match emojis
    	 Pattern emojiPattern = Pattern.compile("[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{1F700}-\\x{1F77F}\\x{1F780}-\\x{1F7FF}\\x{1F800}-\\x{1F8FF}\\x{1F900}-\\x{1F9FF}\\x{1FA00}-\\x{1FA6F}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{2300}-\\x{23FF}\\x{2B50}\\x{200D}\\x{23CF}\\x{23E9}-\\x{23F4}\\x{1F3C3}-\\x{1F3CC}\\x{1F3F3}\\x{1F525}\\x{1F30D}-\\x{1F567}\\x{1F680}-\\x{1F6C5}\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{1F004}\\x{1F0CF}\\x{1F004}\\x{1F600}-\\x{1F64F}\\x{1F680}-\\x{1F6FF}\\x{1F1E6}-\\x{1F1FF}\\x{1F170}-\\x{1F251}\\x{1F910}-\\x{1F93E}\\x{1F004}\\x{1F469}-\\x{1F93E}\\x{1F004}\\x{1F9B0}-\\x{1F9B3}\\x{1F3FB}-\\x{1F3FF}\\x{1F004}\\x{2705}\\x{2611}\\x{2714}\\x{1F929}-\\x{1F932}\\x{1F3F3}\\x{1F4AA}\\x{1F4AA}\\x{1F1E6}-\\x{1F1FF}\\x{1F600}-\\x{1F64F}\\x{1F1E6}-\\x{1F1FF}]");
    	 return emojiPattern.matcher(token).matches();
    }}