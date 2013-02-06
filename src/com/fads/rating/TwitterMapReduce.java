package com.fads.rating;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TwitterMapReduce {
static BufferedReader brpositive = null;
    static BufferedReader brnegtive = null;
       static FileReader fspos= null;
static FileReader fsneg = null;

static public class TweetMap extends Mapper<LongWritable, Text, Text,Text> {
public void map(LongWritable key, Text value, Context context)  {
final Text erskey = new Text("49ers");
final Text ravenskey = new Text("ravens");
Text tweetval = new Text();
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
//if(tweet.contains("#revans"))
//{   
tweetval.set(tweet);
context.write(erskey,tweetval);
//} //if
}
/*if(tweet.contains("#ravens")) {
tweetval.set(tweet);
context.write(ravenskey,tweetval);
}*/ //if
if(tweet.contains("ravens")) {
tweetval.set(tweet);
context.write(ravenskey,tweetval);
} //if
}//while
}//else
}catch(Exception e)
{
}
}//map
}//mapper
static public class TweetReducer extends Reducer<Text,Text,Text,Text> {
static Configuration config = HBaseConfiguration.create();
static HTable table;
public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
	table = new HTable(config, "TwitterTweets");
          
String temp = null;
String tweettemp = null;
Double positive49 = new Double("0");
Double negitive49 =new Double("0");
Double positivere = new Double("0");
Double negitivere = new Double("0");
//Double total49 = 1L;
Double neutral49 = new Double("0");
Double neutralre =new Double("0");
Double count49=new Double("0");
Double countre=new Double("0");
Path pt1=new Path("/user/hadoop/positive-words.txt");
            FileSystem fs1 = FileSystem.get(new Configuration());
             brpositive=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
             System.out.println(" method open streamsss");
             Path pt2 = new Path("/user/hadoop/negative-words.txt");
             FileSystem fs2 = FileSystem.get(new Configuration());
              brnegtive =new BufferedReader(new InputStreamReader(fs2.open(pt2)));

            
            
HTable table = new HTable(config, "TwitterTweets");
if(key.toString().trim().equalsIgnoreCase("49ers")){
Put p = new Put(Bytes.toBytes("49ers")); 
for(Text t: values){
count49++;
tweettemp = t.toString();
             
while(brpositive.ready()) {
temp =  brpositive.readLine();
            
             if(tweettemp.contains(temp))
             positive49 ++;
} 
             
              while( brnegtive.ready()) {
             temp =  brnegtive.readLine();
             if(temp != null)
             if(tweettemp.contains(temp))
             negitive49 ++;
            
              }
             
             
             
              
              
 
               p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("49ers"),Bytes.toBytes(t.toString()));
              table.put(p);
                 context.write(key,t);
}    
//for
Double positiveper=new Double("0");
Double negper=new Double("0");
Double neuper=new Double("0");

			neutral49= count49-(positive49+negitive49);
			
			
			positiveper= (Double)positive49/(positive49+negitive49+neutral49)*100;
			negper= (Double)negitive49/(positive49+negitive49+neutral49)*100;
			neuper= (Double)neutral49/(positive49+negitive49+neutral49)*100;




			p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("positive"),Bytes.toBytes(positiveper.toString()));
            table.put(p);
          
            p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("negitive"),Bytes.toBytes(negper.toString()));
            table.put(p);
            p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("neutral"),Bytes.toBytes(neuper.toString()));
            table.put(p);
}//if
             
              
               else if(key.toString().trim().equalsIgnoreCase("ravens"))
               {
              Put p = new Put(Bytes.toBytes("ravens"));
              for(Text t: values){
             
              countre++;
             
              tweettemp = t.toString();
                      
          
           while(( temp = brpositive.readLine()) != null) {
                        
                        if(tweettemp.contains(temp))
                        positivere ++;
           } 
                    
                     while(( temp =  brnegtive.readLine()) != null) {
                        
                        if(tweettemp.contains(temp))
                        negitivere ++;
                        
                     }
                    
          
             
              //Put p = new Put(Bytes.toBytes("ravens"));     
                   p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("ravens"),Bytes.toBytes(t.toString()));
                  table.put(p);
                  context.write(key,t);
              }//for
             
              neutralre =countre -(positivere+negitivere);
          
              Double positiveper= (Double)(positivere/(neutralre+positivere+negitivere))*100;
              Double negper= (Double)(negitivere/(neutralre+positivere+negitivere))*100;
                  Double neuper= (Double)(neutralre/(neutralre+positivere+negitivere))*100;
                  p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("positive"),Bytes.toBytes(positiveper.toString()));
                   table.put(p);
                  
                   p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("negitive"),Bytes.toBytes(negper.toString()));
                   table.put(p);
                   p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("neutral"),Bytes.toBytes(neuper.toString()));
                   table.put(p);
             
             
               }//else if
brnegtive.close();
brpositive.close();
}//reduce
}//reducer
/*static public void openStream() {
try{
Path pt1=new Path("hdfs://192.168.1.7:54310/user/hadoop/positive-words.txt");
            FileSystem fs1 = FileSystem.get(new Configuration());
             brpositive=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
             System.out.println(" method open streamsss");
         Path pt2 = new Path("hdfs://192.168.1.7:54310/user/hadoop/negative-words.txt");
         FileSystem fs2 = FileSystem.get(new Configuration());
          brnegtive =new BufferedReader(new InputStreamReader(fs2.open(pt2)));
}
catch( Exception e){
}
}*/
 
//streams
public static void main(String[] args) throws Exception{
//openStream();
Configuration conf = new Configuration();
Job job = new Job(conf,"Twitter");
     job.setJarByClass(TwitterMapReduce.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
         
      job.setMapperClass(TweetMap.class);
      job.setReducerClass(TweetReducer.class);
         
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
         String s=args[0];
         String ss=args[1];
      FileInputFormat.addInputPath(job, new Path(s));
      FileOutputFormat.setOutputPath(job,new Path(ss));
       
      job.waitForCompletion(true);
}
}
//class

