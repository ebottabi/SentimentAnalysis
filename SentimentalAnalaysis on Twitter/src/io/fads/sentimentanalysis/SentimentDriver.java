package io.fads.sentimentanalysis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SentimentDriver {
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		
			Job job = new Job(conf,"SentimentAnalysis");
				
				
		      job.setJarByClass(SentimentDriver.class);
		      job.setOutputKeyClass(Text.class);
		      job.setOutputValueClass(Text.class);
		       
		      job.setMapperClass(SentimentMap.class);
		      job.setNumReduceTasks(24);
		     // job.setCombinerClass(SentimentReducer.class);
		      job.setReducerClass(SentimentRed.class);
		      
		      
		      job.setMapOutputKeyClass(Text.class);
		      job.setMapOutputValueClass(Text.class);
		      
		     
		      job.setNumReduceTasks(10);
		         
		      job.setInputFormatClass(TextInputFormat.class);
		      job.setOutputFormatClass(TextOutputFormat.class);
		         String s=args[0];
		         String ss=args[1];
		      FileInputFormat.addInputPath(job, new Path(s));
		      FileOutputFormat.setOutputPath(job,new Path(ss));
		       
		      job.waitForCompletion(true);
		}
}