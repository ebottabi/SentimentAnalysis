package io.fads.sentimentanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.RandomAccess;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



	
	public class SentimentRed extends Reducer<Text,Text,Text,Text> implements RandomAccess
	{
	    Path posfilepath;
	    Path negfilepath;
	   
	    BufferedReader posbuffreader;
	    BufferedReader negbuffreader;
	   
	    static Double totalravrecords=new Double("0");
	    static Double total49ersrecords=new Double("0");
	   
	    static Double ravensnegativecnt=new Double("0");
	    static Double ravenspositivecnt=new Double("0");
	    static Double ravensneutralcnt=new Double("0");
	   
	    static Double ravensnegpercent=new Double("0");
	    static Double ravenspospercent=new Double("0");
	    static Double ravensneupercent=new Double("0");
	   
	    static Double c49ersnegativecnt=new Double("0");
	    static Double c49erspositivecnt=new Double("0");
	    static Double c49ersneutralcnt=new Double("0");
	   
	    static Double c49ersnegpercent=new Double("0");
	    static Double c49erspospercent=new Double("0");
	    static Double c49ersneupercent=new Double("0");

	    Pattern pattern;
	    Matcher matcher;
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	    {
	        String check1="ravens";
	        String check2="49ers";
	       
	        HTable table;
	        Configuration config = HBaseConfiguration.create();

	                              /*Positive file loading*/
	            posfilepath=new Path("/user/hadoop/positive-words.txt");
	            FileSystem fs1 = FileSystem.get(new Configuration());
	            posbuffreader=new BufferedReader(new InputStreamReader(fs1.open(posfilepath)));
	           
	                             /*Negative file loading*/
	             negfilepath = new Path("/user/hadoop/negative-words.txt");
	             FileSystem fs2 = FileSystem.get(new Configuration());
	             negbuffreader =new BufferedReader(new InputStreamReader(fs2.open(negfilepath)));
	       
	        if(check1.equals(key.toString()))
	        {
	            for(Text twit:values)
	            {
	                ++totalravrecords;
	               
	                boolean flag1=false;
	                boolean flag2=false;
	               
	                String regex1 = "";
	                String regex2 = "";
	                while(posbuffreader.ready())
	                {
	                    regex1=posbuffreader.readLine().trim();
	                    pattern = Pattern.compile(regex1, Pattern.CASE_INSENSITIVE);
	                    matcher = pattern.matcher(twit.toString());
	                    flag1=matcher.find();
	                    if(flag1)
	                    {
	                        break;
	                    }       
	                }//while
	               
	                while(negbuffreader.ready())
	                {
	                    regex2=negbuffreader.readLine().trim();
	                    pattern = Pattern.compile(regex2, Pattern.CASE_INSENSITIVE);
	                    matcher = pattern.matcher(twit.toString());
	                    flag2=matcher.find();
	                    if(flag2)
	                    {
	                        break;
	                    }
	                       
	                }//while
	                if(flag1==false&flag2==false)
                    {
                    	++ravensneutralcnt;
                    }
	                if(flag1&flag2)
	                {
	                    ++ravensneutralcnt;
	                }
	                
	                else 
	                {
	                    if(flag1)
	                    {
	                        ++ravenspositivecnt;
	                    }
	                    if(flag2)
	                    {
	                        ++ravensnegativecnt;
	                    }
	                  
	                }
	               
	            }//for
	            ravensnegpercent=ravensnegativecnt/totalravrecords*100;
	            ravenspospercent=ravenspositivecnt/totalravrecords*100;
	            ravensneupercent=ravensneutralcnt/totalravrecords*100;
	        
	
	
	        Put p = new Put(Bytes.toBytes("ravens"));
	
	        table = new HTable(config, "TwitterTweets");
            p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("positive%"),Bytes.toBytes(ravenspospercent.toString()));
             table.put(p);
            
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("negitive%"),Bytes.toBytes(ravensnegpercent.toString()));
             table.put(p);
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("neutral%"),Bytes.toBytes(ravensneupercent.toString()));
             table.put(p);
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("Total Revans"),Bytes.toBytes(totalravrecords.toString()));
             table.put(p);
      // conn.closeTable();
        table.close();
	
	        }//if
	
	    
	
	    if(check2.equals(key.toString()))
        {
           
                for(Text twit:values)
                {
                    ++total49ersrecords;
                   
                    boolean flag1=false;
                    boolean flag2=false;
                   
                    String regex1 = "";
                    String regex2 = "";
                    while(posbuffreader.ready())
                    {
                        regex1=posbuffreader.readLine().trim();
                        pattern = Pattern.compile(regex1, Pattern.CASE_INSENSITIVE);
                        matcher = pattern.matcher(twit.toString());
                        flag1=matcher.find();
                        if(flag1)
                        {
                            break;
                        }       
                    }
                   
                    while(negbuffreader.ready())
                    {
                        regex2=negbuffreader.readLine().trim();
                        pattern = Pattern.compile(regex2, Pattern.CASE_INSENSITIVE);
                        matcher = pattern.matcher(twit.toString());
                        flag2=matcher.find();
                        if(flag2)
                        {
                            break;
                        }
                           
                    }
                    
                    
                    if(flag1==false&flag2==false)
                    {
                    	++c49ersneutralcnt;
                    }
                    if(flag1&flag2)
                    {
                        ++c49ersneutralcnt;
                    }
                    else
                    {
                        if(flag1)
                        {
                            ++c49erspositivecnt;
                        }
                        if(flag2)
                        {
                            ++c49ersnegativecnt;
                        }
                        
                    }
                   
                }//for
            c49ersnegpercent=(c49ersnegativecnt/(total49ersrecords))*100;
            c49erspospercent=(c49erspositivecnt/(total49ersrecords))*100;
            c49ersneupercent=(c49ersneutralcnt/(total49ersrecords))*100;
        
             
	    
	    Put p = new Put(Bytes.toBytes("49ers"));


        table = new HTable(config, "TwitterTweets");
            p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("positive%"),Bytes.toBytes(c49erspositivecnt.toString()));
             table.put(p);
            
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("negitive%"),Bytes.toBytes(c49ersnegpercent.toString()));
             table.put(p);
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("neutral%"),Bytes.toBytes(c49ersneupercent.toString()));
             table.put(p);
             p.add(Bytes.toBytes("Tweets"),Bytes.toBytes("Total Revans"),Bytes.toBytes(total49ersrecords.toString()));
             table.put(p);
      // conn.closeTable();
        table.close();
	    
        }//if
    }//reduce(-,-,-)
}//reducer class
