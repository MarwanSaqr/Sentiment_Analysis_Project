import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SentimentAnalysisDriver {
	public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SentimentAnalysisDriver.class);
        job.setJobName("Sentiment Analysis");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SentimentAnalysisMapper.class);
        job.setReducerClass(SentimentAnalysisReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}