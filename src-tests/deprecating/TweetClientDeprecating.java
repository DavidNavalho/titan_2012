package deprecating;

import static sys.Sys.Sys;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import sys.dht.api.DHT;
import tests.mixed_comm.DataExtractor;
import titan.gateway.client.setup.TotalSetFactory;
import titan.gateway.client.setup.TotalWordsSetKeyFactory;
import titan.gateway.client.setup.TweetSetFactory;
import titan.gateway.client.setup.TweetSetKeyFactory;
import titan.gateway.client.setup.TweetWCSetKeyFactory;
import titan.gateway.client.setup.WCTweetSetFactory;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;
import titan.sys.SysHandler;
import titan.sys.SysNode;
import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;
import titan.sys.data.SysmapManager;
import titan.sys.data.Tweet;
import titan.sys.data.triggers.TotalWordsCountTrigger;
import titan.sys.data.triggers.Trigger;
import titan.sys.data.triggers.TweetSetTriggerToWordCount;
import titan.sys.data.triggers.WordCountToTopTrigger;
import titan.sys.messages.SysmapCreationMessage;
import titan.sys.messages.SysmapRequestMessage;
import titan.sys.messages.TriggerCreationMessage;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.replies.SysmapReplyMessage;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
//import static sys.utils.Log.Log;
//import sys.utils.Log;

public class TweetClientDeprecating {

	private DHT stub;
	private boolean isClient;
	
	//need: multiple clients, pre-loaded with tweets, sending chunks of updates at a time
	//test with 1 client with full upload to 1 set first
	public TweetClientDeprecating(boolean isClient) {
		this.isClient = isClient;
	}
	
	public void startServer(){
		SysNode sysnode = new SysNode();
//		sysnode.newSet("TweetSet", 4, "jinx");
//		sysnode.newSet("TweetsHeatMap",1,"jinx");
        sysnode.initialize();
	}
	
	public void startSetManagement(){
//		Log.setLevel(Level.ALL);
		sys.Sys.init();
        stub = Sys.getDHT_ClientStub();
        String console = "SetManager> ";
//        System.out.println(console+"Set Management initialized.");
	}
	
	public void addSetToSystem(String setName, PartitionKeyFactory setKey, int numberOfPartitions, SetFactory factory){
//		SysmapCreationMessage(String messageType, String setName, PartitionKeyFactory setKey, int numberOfPartitions, SetFactory factory)
		String console = "SetManager> ";
		SysmapCreationMessage msg =  new SysmapCreationMessage("registerSet", setName, setKey, numberOfPartitions, factory);
		System.err.println("Client> Sending SetCreation request: "+setName);
		this.stub.send(new SysKey(setName), msg);
//		System.out.println(console+"New set creation request: "+setName+".");
	}
	
	public void addTriggers(Sysmap sysmap){
		String console = "SetManager> ";
		//request sysmap for trigger creation!//only then start the actual client...
		Trigger trigger = new TweetSetTriggerToWordCount(sysmap, 0, 100);
		TriggerCreationMessage msg = new TriggerCreationMessage(trigger,"TweetSet");
		this.stub.send(new SysKey("TweetSet"), msg);
		System.out.println(console+"WordCount Trigger sent to the system");
		
//		Trigger trigger = new WordCountToTopTrigger(targetSet, top)
	}
	
	public void addWCTrigger(Sysmap sysmap){
		String console = "SetManager> ";
		int top = 50;
		Trigger trigger = new WordCountToTopTrigger(sysmap, top, 0, 1);
		TriggerCreationMessage msg = new TriggerCreationMessage(trigger, "TweetWordCountSet");
		this.stub.send(new SysKey("TweetWordCountSet"), msg);
//		System.out.println(console+"Top "+top+" WordCount Trigger sent to the System");
	}
	
	public void addTotalTrigger(Sysmap sysmap){
		String console = "SetManager> ";
		Trigger trigger = new TotalWordsCountTrigger(sysmap, 0, 10);
		trigger.setTriggerName("TotalWordCount");
		TriggerCreationMessage msg = new TriggerCreationMessage(trigger, "TweetWordCountSet");
		this.stub.send(new SysKey("TweetWordCountSet"), msg);
//		System.out.println(console+"Total WordCount Trigger sent to the System");
	}
	
	public void setManagement(){
		//create a TweetSet
		String setName = "TweetSet";
		TweetSetKeyFactory setKey = new TweetSetKeyFactory();
		int numberOfPartitions = 4;
		SetFactory factory = new TweetSetFactory();
		this.addSetToSystem(setName, setKey, numberOfPartitions, factory);
		//create wordCount Set
		String wcSetName = "TweetWordCountSet";
		TweetWCSetKeyFactory wcSetKey = new TweetWCSetKeyFactory();
		int wcNumberOfPartitions = 2;
		SetFactory wcFactory = new WCTweetSetFactory();
		this.addSetToSystem(wcSetName, wcSetKey, wcNumberOfPartitions, wcFactory);
		//create top wordcount set (its basically a wordcount set...)
//		String topSetName = "TopWordCountSet";
//		TweetWCSetKeyFactory topSetKey = new TweetWCSetKeyFactory();
//		int topNumberOfPartitions = 1;
//		SetFactory topFactory = new TopSetFactory();
//		this.addSetToSystem(topSetName, topSetKey, topNumberOfPartitions, topFactory);
		//create a total words set
		String totalSetName = "TotalWordsCountSet";
		TotalWordsSetKeyFactory totalSetKey = new TotalWordsSetKeyFactory();
		int totalNumberOfPartitions = 1;
		SetFactory totalFactory = new TotalSetFactory();
		this.addSetToSystem(totalSetName, totalSetKey, totalNumberOfPartitions, totalFactory);
		this.stub.send(new SysKey(wcSetName), new SysmapRequestMessage("sysmapRequest",wcSetName), this.handler);
//		/*this.stub.send(new SysKey(topSetName), new SysmapRequestMessage("sysmapRequest",topSetName), this.handler);*/
		this.stub.send(new SysKey(totalSetName), new SysmapRequestMessage("sysmapRequest",totalSetName), this.handler);
		
	}
	//###############CLIENT STUFF BELOW
	
	
	
	public void startClient(LinkedList<Tweet> tweets){
		this.tweets = tweets;
//		Log.setLevel(Level.ALL);
		sys.Sys.init();
        stub = Sys.getDHT_ClientStub();
        String console = "Client> ";
//        System.out.println(console+"Client initialized!");
	}
	
	
	ClientHandler handler = new ClientHandler();
	LinkedList<Tweet> tweets = new LinkedList<Tweet>();
	
	public void prepareTweets(int nTweets, int skip){
		try{
			int counter=0;
			Mongo bigMongo = new Mongo();
			DB mongoDB = bigMongo.getDB("twitPushDB");
			DBCollection twitCollection = mongoDB.getCollection("twitPushCollection");
//			DBCollection twitErrorCollection = mongoDB.getCollection("twitPushErrorCollection");
//			DBCollection mongoCollection = mongoDB.getCollection("mongoErrorsCollection");
			Iterator<DBObject> it = twitCollection.find().iterator();
			while(it.hasNext()){
				DBObject obj = it.next();
				String id = ((Long)obj.get("Id")).toString();
				Date date = (Date)obj.get("CreatedAt");
				Double latitude = (Double)obj.get("geoLatitude");
				Double longitude = (Double)obj.get("geoLongitude");
				String text = (String)obj.get("Text");
				String user = (String)obj.get("User");
				if(latitude!=null && longitude!=null){
					counter++;
					Tweet newTweet = new Tweet(text,latitude,longitude,user,date,id);
					if(skip<nTweets)
						this.tweets.add(newTweet);
				}
				if(counter>=nTweets+skip)
					return ;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return;
	}
	
	
	
	public void getSysmap(String setName){
		//first, get a sysmap
		String console = "Client> ";
		this.stub.send(new SysKey(setName), new SysmapRequestMessage("sysmapRequest",setName), this.handler);
//	    System.out.println(console+"Sysmap request sent.");
	}
	
	private Map<Long, LinkedList<Tweet>> partitionNodes;
	
	private LinkedList<LinkedList<Tweet>> scatteredTweets;
	
//	private void sendTweets(Sysmap sysmap, LinkedList<Tweet> tweetToSend){
//		SetManager manager = new SetManager(sysmap);
//		for (Tweet tweet : tweetToSend) {
//			manager.addData(tweet, tweet.getTweetKey());
//		}
//		manager.syncAndDiscard(this.stub);
//	}
	
	private String filePath = "/Users/jinx/libs/data/icwsm_2011/checkin_data.txt";
	private int totalTweetsToSend = 1000000;
	private int tweetsPerMessage = 100;
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public void setTotalTweetsToSend(int totalTweetsToSend) {
		this.totalTweetsToSend = totalTweetsToSend;
	}
	
	public void setTweetsPerMessage(int tweetsPerMessage) {
		this.tweetsPerMessage = tweetsPerMessage;
	}
	
	public void sendTweetsFromFile(Sysmap sysmap){
		if(sysmap.getSetName().equalsIgnoreCase("TweetSet")){
			try {
				int counter = 0;
				while(counter<totalTweetsToSend){
					DataExtractor de = new DataExtractor(this.filePath);
					SysmapManager manager = new SysmapManager(sysmap);
					for(int i=0;i<tweetsPerMessage;i++){
						Tweet tweet = de.readTweet();
						manager.addData(tweet, tweet.getTweetKey());
						counter++;
					}
//DEPRECATED	manager.syncAndDiscard(stub);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}else{
			System.err.println("Client> Did not receive a TweetSet...");
		}
	}
	
	//TODO: still missing the sysmap handlers on the client side...it's done manually for now...
	public void sendBulkTweets(Sysmap sysmap, int tweetsPerMessage){
		String console = "Client> ";
		if(sysmap.getSetName().equalsIgnoreCase("TweetSet")){
			int totalTweets = this.tweets.size();
			int currentTweet = 0;
			
			SysmapManager manager = new SysmapManager(sysmap);
			for (Tweet tweet : this.tweets) {
				manager.addData(tweet, tweet.getTweetKey());
			}
//			System.out.println(console+"Tweets added to local sets, syncing...");
//DEPRECATED	manager.syncAndDiscard(this.stub);
//			System.out.println(console+"tweets synced.");
			/*
			PartitionKeyFactory partitionsManager = sysmap.getKeyMaker();
			SetFactory setManager = sysmap.getSetCreator();
//			int totalPartitions = sysmap.partitions();
			this.partitionNodes = new HashMap<Long, LinkedList<Tweet>>();
//			LinkedList<SysSet> partitions = new LinkedList<SysSet>();
//			for(int i=0;i<totalPartitions;i++){
//				partitions.add(setManager.createEmpty());
//			}
			LinkedList<LinkedList<Tweet>> tweetSets = new LinkedList<LinkedList<Tweet>>();
			for (Tweet tweet : this.tweets) {
				SysKey tweetKey = tweet.getTweetKey();
				Long partitionsKey = partitionsManager.getPartitionKey(tweetKey, sysmap.getSetName(), sysmap.partitions());
				if(!this.partitionNodes.containsKey(partitionsKey)){
					LinkedList<Tweet> newTweetList = new LinkedList<Tweet>();
					this.partitionNodes.put(partitionsKey, newTweetList);
					tweetSets.add(newTweetList);
				}
				LinkedList<Tweet> tweetSet = this.partitionNodes.get(partitionsKey);
				tweetSet.add(tweet);
			}
			int nextPartition=1;
			for (LinkedList<Tweet> linkedList : tweetSets) {
				Long partitionKey = partitionsManager.getPartitionKey(nextPartition, "TweetSet", sysmap.partitions());
				this.stub.send(new SysKey(partitionKey.toString()), new DataDeliveryMessage("addData",linkedList,partitionKey));
				nextPartition++;
			}
//			int totalTweets = 0;
//			for (LinkedList<Tweet> linkedList : tweetSets) {
//				for (Tweet tweet : linkedList) {
//					totalTweets++;
//				}
//			}
//			System.out.println(console+"Total local partitions: "+tweetSets.size()+" and tweets: "+totalTweets);
		*/	
		}
		//else do nothing, i guess...
	}
	
	
	
	
	public static void main(String[] args) {
//		TweetClient server = new TweetClient(false);
//		server.startServer();
		
		TweetClientDeprecating manager = new TweetClientDeprecating(false);
		manager.startSetManagement();
		manager.setManagement();
	
//		try {
//			Thread.sleep(15000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		TweetClient client = new TweetClient(true);
//		String console ="Client> ";
//		System.out.println("Client initializing...");
//		client.startClient();
//		client.prepareTweets(100,0);
//		System.out.println(console+client.tweets.size()+" Tweets prepared to send...");
//		client.getSysmap("TweetSet");
		
//		TweetGenerator clientTweets = new TweetGenerator(1,100);
//		LinkedList<LinkedList<Tweet>> tweetLists = clientTweets.getAllTweets();
//		try {
//			System.in.read();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		String console = "Client>";
////		System.out.println("Clients initializing...");
////		Log.finest(console+" started @ "+System.currentTimeMillis());
//		System.out.println(console+" started @ "+System.currentTimeMillis());
//		for(int i=0;i<1;i++){//TODO ??
//			TweetClient client = new TweetClient(true);
//			if(args.length==3){
//				int totalTweets = new Integer(args[0]);
//				int tweetsPerMessage = new Integer(args[1]);
//				String filePath = args[2];
//				client.setTotalTweetsToSend(totalTweets);
//				client.setTweetsPerMessage(tweetsPerMessage);
//				client.setFilePath(filePath);
//			}
////			client.startClient(null);
////			client.getSysmap("TweetSet");
//		}
//		for (LinkedList<Tweet> tweetList : tweetLists) {
//			TweetClient client = new TweetClient(true);
//			client.startClient(tweetList);
//			client.getSysmap("TweetSet");
//		}
	}
	
	
	
	private class ClientHandler extends SysHandler.ReplyHandler{
		private String console = "Client> ";
		private Sysmap sysmap = null;
		//TODO: now that I have a sysmap, I have to use it....
		@Override
		public void onReceive(SysMessageReply reply) {
			System.out.println(console+"Reply ack: "+reply.messageType);
			if(reply.messageType.equalsIgnoreCase("sysmapReply")){
				SysmapReplyMessage msg = (SysmapReplyMessage) reply;
				sysmap = msg.getSysmap();
//				System.out.println(console+"Sysmap received: "+sysmap.getSetName());
//				System.out.println(console+"Sysmap received by client: "+sysmap.partitions()+" partitions.");
				if(!isClient){
					if(sysmap.getSetName().equalsIgnoreCase("TweetWordCountSet")){
						addTriggers(sysmap);
					}else{
//						addWCTrigger(sysmap);
						addTotalTrigger(sysmap);
					}
				}
				else{
//						sendBulkTweets(sysmap, 100);
//					sendTweetsFromFile(sysmap);
				}
				
			}
		}
		@Override
		public void onReceive(SetCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void onReceive(SysmapCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
	}
	
}
