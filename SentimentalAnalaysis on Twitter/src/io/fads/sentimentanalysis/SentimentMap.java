package io.fads.sentimentanalysis;

import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SentimentMap extends Mapper<LongWritable, Text, Text,Text> {
	
	final Text erskey = new Text("49ers");
	final Text ravenskey = new Text("ravens");
	Text tweetval = new Text();
	public void map(LongWritable key, Text value, Context context)  {
		
		
		try{
			
		if(value == null){
		return;
		} else {
			
			StringTokenizer tokens = new StringTokenizer(value.toString(),",");
			int count = 0;
				while(tokens.hasMoreTokens()) {
					count ++;
						if(count <=1)
							continue;
							String tweet = tokens.nextToken().toLowerCase().trim();
		if(tweet.contains("49ers")) {
		
		   
		tweetval.set(tweet);
		context.write(erskey,tweetval);
				}
		
		if(tweet.contains("ravens")) {
		tweetval.set(tweet);
		context.write(ravenskey,tweetval);
		} //if
		}//while
		}//else
		}catch(Exception e){
			}
		}//map

}