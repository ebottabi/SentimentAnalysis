package com.fads.sentimentanalysis;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


public class KeyBasedTweets {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		 ConfigurationBuilder cb = new ConfigurationBuilder();
	        cb.setOAuthConsumerKey("aKBd1E28xgODSjKoQUlQ6Q");
	        cb.setOAuthConsumerSecret("vnyFKk2EzuWyP0Qx7lceBYhoc4M4yCuzXmHoMQLSQFs");
	        cb.setOAuthAccessToken("1127121805-F5La6wNKEy6yEzwSnQ57DfbThhA9S8ZqTahZS1F");
	        cb.setOAuthAccessTokenSecret("     IKUZ5sbDVRa9JXrvA2y6mWDGitNjmMSLdstfGpFk");
	         
	        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		
		
	        FileWriter fstream = new FileWriter("twitterstream.txt",true);
	        BufferedWriter out = new BufferedWriter(fstream);
		    
			while(true)
			{

			for (int page = 1; page <= 15; page++) {
			System.out.println("\nPage: " + page);
			Query query = new Query("#49ers");
			
					query.setQuery("#superbowl");
					query.setQuery("#nfl");
					query.setQuery("#football");
					query.setQuery("#defence");
					query.setQuery("#affence");
					query.setQuery("#quarterback");
					query.setQuery("#touchdown");
					query.setQuery("#baltimore");
					query.setQuery("#ravens");
					/*query.setQuery("#california");
					query.setQuery("#beyonce");
					query.setQuery("#beyonce novels");
					query.setQuery("#Alex Smith");

					query.setQuery("#Scott Tolzien");*/
					query.setRpp(1000); 
			// set tweets per page to 1000
			query.setPage(page);
			QueryResult qr = twitter.search(query);
			List<Tweet> qrTweets = qr.getTweets();

			// break out of the loop early if there are no more tweets
			if(qrTweets.size() == 0) break;

			for(Tweet t : qrTweets) {
			System.out.println(t.getId() + " - " + t.getCreatedAt() + ": " + t.getText());
			
			
		
			out.write("\n"+t.getId()+",");
            out.write("\t"+t.getText()+",");
           // out.write("\t"+t.getLocation()+",");
            out.write("\t"+t.getFromUser()+",");
            //out.write("\t"+user.toString());
            
			}
			}
			try{
				Thread.sleep(1000*60*15);
			}catch(Exception e) {}
			
	}//while

}

}
