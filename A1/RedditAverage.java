import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.json.JSONObject;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
		String input_string = value.toString();
        JSONObject data = new JSONObject(input_string);
     

    	Text outputname = new Text(data.get("subreddit").toString());
    	long cnumber = (long)1;
    	String score = data.get("score").toString();
    	LongPairWritable outputvalue = new LongPairWritable(cnumber,Long.parseLong(score));
    	context.write(outputname, outputvalue);
 
			}
		}

	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		
		@Override
	    public void reduce(Text key, Iterable<LongPairWritable> values,
	            Context context
	            ) throws IOException, InterruptedException {
			long tcomments = 0;
			long tscore = 0;
	        for (LongPairWritable value : values) {
	        	tcomments += value.get_0();
	        	tscore += value.get_1();
	        }
	        LongPairWritable redditscore = new LongPairWritable(tcomments, tscore);
	        context.write(key, redditscore);
	    }
	}
	
	
public static class RedditReducer
extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	
	@Override
    public void reduce(Text key, Iterable<LongPairWritable> values,
            Context context
            ) throws IOException, InterruptedException {
		
		long comments = 0;
		long score = 0;
		double averagescore = 0;
        for (LongPairWritable pair : values) {
        	comments += pair.get_0();
        	score += pair.get_1();
        }
        averagescore =(double) score/comments;
        result.set(averagescore);
        context.write(key, result);
    }
}
	
public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
	System.exit(res);
}

@Override

public int run(String[] args) throws Exception {
	Configuration conf = this.getConf();
	Job job = Job.getInstance(conf, "RedditAverage");
	job.setJarByClass(RedditAverage.class);

	job.setInputFormatClass(TextInputFormat.class);

	job.setMapperClass(RedditMapper.class);
	job.setCombinerClass(RedditCombiner.class);
	job.setReducerClass(RedditReducer.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongPairWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	TextInputFormat.addInputPath(job, new Path(args[0]));
	TextOutputFormat.setOutputPath(job, new Path(args[1]));

	
	return job.waitForCompletion(true) ? 0 : 1;
    }

}



