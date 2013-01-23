package titan.gateway.client;

import static sys.Sys.Sys;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.dht.api.DHT;
import titan.data.DataManager;
import titan.sys.SysHandler;
import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;
import titan.sys.data.Tweet;
import titan.sys.messages.SysmapRequestMessage;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.replies.SysmapReplyMessage;
import titan.sys.messages.rpc.RpcReply;
import utils.ClientsManager;
import dataSources.ITweetClient;


public class GatewayClient {
	
	protected DHT stub;
	protected SysHandler.ReplyHandler asyncHandler;
	protected BlockingQueue<Sysmap> resourceLocations;
	
	public GatewayClient() {
//		Log.setLevel(Level.ALL);
		this.resourceLocations = new LinkedBlockingQueue<Sysmap>();
		this.asyncHandler = new GWHandler();
		sys.Sys.init();
		stub = Sys.getDHT_ClientStub();
	}
	
	//TODO: returning null, and error catching isn't great...
	public Sysmap requestSysmap(String resourceName){
		this.stub.send(new SysKey(resourceName), new SysmapRequestMessage("sysmapRequest", resourceName), this.asyncHandler);
		try {
			return this.resourceLocations.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	public static void main(String[] args) {
		//defaults
		int maxWaitTime = 1000;
		int tweetsToRetrieve = 100;
		String path = "ClientLocations.txt";
		//done
		if(args.length==3){
			maxWaitTime = new Integer(args[1]);
			tweetsToRetrieve = new Integer(args[2]);
			path = args[0];
		}else if(args.length!=0){
			System.err.println("Incorrect usage: GatewayClient sourcesLocationsFile maxWaitTime tweetsToRetrieve");
			System.exit(0);
		}
		//correct usage:
		//get a sysmap - ideally, should be synchronous!
		GatewayClient gw = new GatewayClient();
		Sysmap sysmap = gw.requestSysmap("TweetSet");
		System.out.println("$ "+sysmap);
		//create a DataManager from the sysmap -> TODO: GWClient should actually use a protocol for this, but for testing purposes now, it should work
		DataManager dm = new DataManager(sysmap, maxWaitTime, tweetsToRetrieve);
		new Thread(dm).start();
		try{
			ArrayList<ITweetClient> clients;
			ClientsManager cm = new ClientsManager(path);
	        clients = cm.getClientsServices();
//			int tweetsToRetrieve2 = 1000;
			int sourcePos = 0;
			//feed the DataManager:
			int testMax = 50000;
			while(true){
				if(testMax<=0){
					System.out.println("50k tweets done, breaking...");
					dm.syncAndDiscard();
					return;
				}
				testMax--;
				sourcePos++;
				if(sourcePos%clients.size()==0)
					sourcePos = 0;
				//Connect to Sources;
				LinkedList<Tweet> tweets = clients.get(sourcePos).getTweets(tweetsToRetrieve);
				for (Tweet tweet : tweets) {
					dm.addData(tweet, tweet.getTweetKey());
				}
				//continuously read X tweets from the sources
				//and add them to the DataManager
				//The DataManager should handle the incoming data (syncs, etc)
					//OR, it can be explicit....
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private class GWHandler extends SysHandler.ReplyHandler{
		
		@Override
		public void onReceive(SysMessageReply reply) {
			Sysmap sysmap = ((SysmapReplyMessage) reply).getSysmap();
			try {
				if(resourceLocations==null)
					System.out.println("Empty resource locations?!");
				resourceLocations.put(sysmap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void onReceive(SysmapCreateReply reply) {
			//TODO: do I come here?
		}
		
		@Override
		public void onReceive(SetCreateReply reply) {
			System.err.println(reply.getSetName());
		}

		@Override
		public void onReceive(RpcReply reply) {
			// TODO Auto-generated method stub
			
		}
	}
}
