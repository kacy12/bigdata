

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

			
			public class WikipediaPopular extends Configured implements Tool {

				public static class WikiMapper
				extends Mapper<LongWritable, Text, Text, LongWritable>{
					
				
				private final static LongWritable count = new LongWritable();
				private Text datewrite = new Text();
				
			@Override	
			public void map(LongWritable key, Text text, Context context
					) throws IOException, InterruptedException {
								
				String line =new String(text.toString());
				String [] eachline = line.toString().split("\\s+");
								
				String wikidate = eachline[0];
				String wikilanguage = eachline[1];
				String wikipage = eachline[2];
				String wikiviews = eachline[3];
				
					if (wikilanguage.startsWith("en") && !(wikipage.equals("Main_Page")) && !wikipage.startsWith("Special:")) {
													
						datewrite.set(wikidate);						
						count.set(Long.parseLong(wikiviews));
						context.write(datewrite, count);
						}
						
					}
}


			 public static class WikiReducer
			extends Reducer<Text, LongWritable, Text, LongWritable> {
				
				private LongWritable result = new LongWritable();

				@Override
				public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
				{
				long pagecount = 0;

				for (LongWritable val : values) {
					if (val.get() > pagecount)
						pagecount = val.get();
				}
				
				result.set(pagecount);
				context.write(key, result);
                }
}
		

			public static void main(String[] args) throws Exception {
				int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
				System.exit(res);
			}

			@Override
			public int run(String[] args) throws Exception {
				Configuration conf = this.getConf();
				Job job = Job.getInstance(conf, "WikipediaPopular");
				job.setJarByClass(WikipediaPopular.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(WikiMapper.class);
			    job.setCombinerClass(WikiReducer.class);
			    job.setReducerClass(WikiReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(LongWritable.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				TextInputFormat.addInputPath(job, new Path(args[0]));
				TextOutputFormat.setOutputPath(job, new Path(args[1]));
			
		        return job.waitForCompletion(true) ? 0 : 1;
			}
		
		}

			
			