package com.fads.twit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class TwitFilter {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
   
    private Text val1;
    private Text key1;
     
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String s=new String(value.toString());
    	//System.out.println("A");
    	//s.split("::");
    	StringTokenizer ss=new StringTokenizer(s,"::");
    	String key12=new String("");
    	while(ss.hasMoreTokens())
    	{
    	key12=ss.nextToken();
    	break;
    	}
    	String val12=new String(s.substring(key12.length()));
    	
    	boolean flag=false;
        String regex = "celetics";
        
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(val12);
        flag=matcher.find();
    	//System.out.println(val12);
    	//while(val12.equals(""))
        if(flag)
        {
    	key1=new Text(key12);
    	val1=new Text(val12);

    	context.write(key1, val1);
        }
    }
  }
 
  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> 
  { 
	  Configuration config = HBaseConfiguration.create(); 
	  HTable table;
	  Put p;
	  static int i;
    public void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException 
        {
    	 		
    	 		table = new HTable(config, "TwitTable");     
    	 		
    	 		p = new Put(Bytes.toBytes("row"+ ++i));      
    	 		for(Text twit:arg1)
    	 		{
    	 			p.add(Bytes.toBytes("UserFamily"), Bytes.toBytes("User"),Bytes.toBytes(arg0.toString()));
    	 			table.put(p);
    	 			p.add(Bytes.toBytes("TwitFamily"), Bytes.toBytes("Twit"),Bytes.toBytes(twit.toString()));
    	 			table.put(p);
    	 			Text t=new Text(twit.toString());
    	 			 arg2.write(arg0,t);  
    	 		}
                
        }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   /* String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: TwitFilter <in> <out>");
      System.exit(2);
    }*/
    Job job = new Job(conf, "word count");
    job.setJarByClass(TwitFilter.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}