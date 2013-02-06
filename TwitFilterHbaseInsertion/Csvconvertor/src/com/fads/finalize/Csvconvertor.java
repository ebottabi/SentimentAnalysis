package com.fads.finalize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.csvreader.CsvReader;

public class Csvconvertor {
public static void main(String args[]) throws IOException
{   
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	
	CsvReader game = new CsvReader("/home/hduser/Downloads/RowFeeder for Celtics and Lakers Game 1.csv");
	System.out.println(game);
	Path outFile = new Path(args[0]);
	
	if (fs.exists(outFile))
		  System.out.println("Output already exists");
		
		
		FSDataOutputStream out = fs.create(outFile);
		//byte[] buffer=new byte[(int)fs.getLength(game)];
		
	
       game.readHeaders();
   
    while (game.readRecord())
    {
        String name1 = game.get("Name");
        
        String update1 = game.get("Update");

        String row=name1+"::"+update1+"\n";
        System.out.println(row);
        out.writeBytes(row);
        
        
    }

    game.close();
	out.close();
}
}
