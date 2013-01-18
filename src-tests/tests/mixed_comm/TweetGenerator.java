package tests.mixed_comm;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import titan.sys.data.Tweet;
import vrs0.crdts.CRDTInteger;
import vrs0.crdts.runtimes.CRDTRuntime;
import vrs0.exceptions.IncompatibleTypeException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class TweetGenerator {
	
	private LinkedList<LinkedList<Tweet>> tweetLists;
	private Iterator<DBObject> it;
	

	//the idea is to keep an incoming flux of tweets sent to multiple system clients
	//so: read N tweets from the database to memory - as many memory data structures holding the tweets as clients
	//then, in parallel, serve the clients with the tweets, one at a time (not waiting for answers, just keep sending them) - this is a simple stress test
	public TweetGenerator(int numberOfClients, int totalNumberOfTweets) {
		this.tweetLists = new LinkedList<LinkedList<Tweet>>();
		for(int i=0;i<numberOfClients;i++){
			this.tweetLists.add(new LinkedList<Tweet>());
		}
		this.connectToMongoDB();
		this.populateTweetLists(numberOfClients, totalNumberOfTweets);
	}
	
	private void populateTweetLists(int numberOfClients, int totalNumberOfTweets){
		int minTweetsPerClient = totalNumberOfTweets/numberOfClients;
		int extraTweets = totalNumberOfTweets%numberOfClients;
		int totalAdded = 0;
		System.out.println("Client Sets being populated: ");
		for(int i=0;i<tweetLists.size();i++){
			int j = 0;
			while(it.hasNext()){
				j++;
				DBObject obj = it.next();
				String id = ((Long)obj.get("Id")).toString();
				Date date = (Date)obj.get("CreatedAt");
				Double latitude = (Double)obj.get("geoLatitude");
				Double longitude = (Double)obj.get("geoLongitude");
				String text = (String)obj.get("Text");
				String user = (String)obj.get("User");
				if(latitude!=null && longitude!=null){
					Tweet newTweet = new Tweet(text,latitude,longitude,user,date,id);
					this.tweetLists.get(i).add(newTweet);
				}else{
					j--;
				}
				if(j>=minTweetsPerClient){
					if(i==0){
						if(j>=(minTweetsPerClient+extraTweets))
							break;
					}else
						break;
				}
			}
			totalAdded += tweetLists.get(i).size();
			System.out.println("\t"+"Client "+(i+1)+": "+tweetLists.get(i).size()+" Tweets added.");
		}
		System.out.println("\t"+"Total tweets added: "+totalAdded);
	}
	
	private void connectToMongoDB(){
		try{
			Mongo bigMongo = new Mongo();
			DB mongoDB = bigMongo.getDB("twitPushDB");
			DBCollection twitCollection = mongoDB.getCollection("twitPushCollection");
			this.it = twitCollection.find().iterator();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public LinkedList<LinkedList<Tweet>> getAllTweets(){
		return this.tweetLists;
	}
	
	public static void main(String[] args) {
		CRDTInteger site1 = new CRDTInteger(0);
		String siteId1 = "site1";
		CRDTRuntime runtime1 = CRDTRuntime.getInstance();
		runtime1.setSiteId(siteId1);
		site1.add(5, runtime1.nextEventClock());
		System.out.println("site1: "+site1.value());
		
		CRDTInteger site2 = new CRDTInteger(5);
		String siteId2 = "site2";
		CRDTRuntime runtime2 = CRDTRuntime.getInstance();
		runtime2.setSiteId(siteId2);
		site2.add(5, runtime2.nextEventClock());
		System.out.println("site2: "+site2.value());
		
		try {
			site1.merge(site2, runtime1.getCausalityClock(), runtime2.getCausalityClock());
			System.out.println("site1 after merging with site2: "+site1.value());
			System.out.println("site2 after merging site2 with site1: "+site2.value());
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();
		}
		
	}
	
}
