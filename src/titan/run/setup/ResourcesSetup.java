package titan.run.setup;

import static sys.Sys.Sys;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.dht.api.DHT;
import titan.gateway.client.setup.TotalSetFactory;
import titan.gateway.client.setup.TotalWordsSetKeyFactory;
import titan.gateway.client.setup.TweetSetFactory;
import titan.gateway.client.setup.TweetSetKeyFactory;
import titan.gateway.client.setup.TweetWCSetKeyFactory;
import titan.gateway.client.setup.WCTweetSetFactory;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;
import titan.sys.SysHandler;
import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;
import titan.sys.data.triggers.TotalWordsCountTrigger;
import titan.sys.data.triggers.Trigger;
import titan.sys.data.triggers.TweetSetTriggerToWordCount;
import titan.sys.messages.SysmapCreationMessage;
import titan.sys.messages.SysmapRequestMessage;
import titan.sys.messages.TriggerCreationMessage;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.replies.SysmapReplyMessage;
import titan.sys.messages.rpc.RpcReply;
//import static sys.utils.Log.Log;

public class ResourcesSetup {
	
	protected DHT stub;
	protected String console = "ResourcesSetup$ ";
	protected SysHandler.ReplyHandler asyncHandler;
	protected BlockingQueue<Sysmap> sysmapQueue;
	protected BlockingQueue<Sysmap> completeSysmapQueue;
	protected BlockingQueue<SetCreateReply> triggerQueue;
	
	public ResourcesSetup() {
		this.defaults();
//		Log.setLevel(Level.ALL);
		this.sysmapQueue = new LinkedBlockingQueue<Sysmap>();
		this.completeSysmapQueue = new LinkedBlockingQueue<Sysmap>();
		this.asyncHandler = new RSHandler();
		sys.Sys.init();
        this.stub = Sys.getDHT_ClientStub();
	}
	
	public int numberOfPartitions, wcNumberOfPartitions, totalNumberOfPartitions, waitTime1, minLoad1, waitTime2, minLoad2;
	public void defaults(){
		//defaults:
		//DataSets
		//TweetSet Partitions
		this.numberOfPartitions = 4;
		//WordsSet Partitions
		this.wcNumberOfPartitions = 2;
		//Final Count Partition -> must be one, so no need to add an argument to it...
		this.totalNumberOfPartitions = 1;
		//Triggers:
		//TweetSet to TweetWordCountSet Trigger
		this.waitTime1 = 50;//in ms
		this.minLoad1 = 100;
		//TweetWordCountSet to TotalWordsCountSet Trigger
		this.waitTime2 = 50;//in ms
		this.minLoad2 = 10;
		//done
	}
	
	public Sysmap createResource(String resourceName, int numberOfPartitions, PartitionKeyFactory resourceKeyFactory, SetFactory resourceFactory){
//		try {
			System.err.println(console+"Creating resource: "+resourceName);
			SysmapCreationMessage scm = new SysmapCreationMessage("registerSet", resourceName, resourceKeyFactory, numberOfPartitions, resourceFactory);
			this.stub.send(new SysKey(resourceName), scm, this.asyncHandler);
			//wait for set to be created...
//			Sysmap sysmap = this.sysmapQueue.take();
//			System.err.println(console+"Resource created: "+sysmap.toString());
//			return sysmap;
//		} catch (InterruptedException e) {
//			// TODO: error handling
//			e.printStackTrace();
//		}
		return null;//TODO: blergh!
	}
	
	public Sysmap getSysmap(String setName){
		System.out.println("Requesting sysmap for: "+setName);
		SysmapRequestMessage srm = new SysmapRequestMessage("sysmapRequest", setName);
		this.stub.send(new SysKey(setName), srm, this.asyncHandler);
		Sysmap sysmap = null;
		try {
			sysmap = this.completeSysmapQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return sysmap;
	}
	
	public void setTrigger(String setName, Trigger trigger){
		System.out.println(console+"Sending "+setName+" trigger to Titan...");
		TriggerCreationMessage msg = new TriggerCreationMessage(trigger,setName);
		this.stub.send(new SysKey(setName), msg, this.asyncHandler);
		//TODO: resume with result...
	}
	
	public void TweetSet(){
		//Create the first Resource: a TweetSet
		//requires: name, #ofPartitions, resourceFactory
		String setName = "TweetSet";
		TweetSetKeyFactory setKey = new TweetSetKeyFactory();
		SetFactory factory = new TweetSetFactory();
		/*Sysmap tweetSetSysmap =*/
		this.createResource(setName, numberOfPartitions, setKey, factory);
		String phrase = "Sent TweetSet creation data. Expected keys:";
		for(int i=1;i<=numberOfPartitions;i++)
			phrase+=" "+setKey.getPartitionKey(i, setName, numberOfPartitions);
		System.out.println(phrase);
	}
	
	public void WordCountSet(){
		//create WordCountSet
		String wcSetName = "TweetWordCountSet";
		TweetWCSetKeyFactory wcSetKey = new TweetWCSetKeyFactory();
		SetFactory wcFactory = new WCTweetSetFactory();
		this.createResource(wcSetName, wcNumberOfPartitions, wcSetKey, wcFactory);
	}
	
	public void TotalWordCountSet(){
		//create TotalWordCountSet
		String totalSetName = "TotalWordsCountSet";
		TotalWordsSetKeyFactory totalSetKey = new TotalWordsSetKeyFactory();
		SetFactory totalFactory = new TotalSetFactory();
		this.createResource(totalSetName, totalNumberOfPartitions, totalSetKey, totalFactory);
	}
	
	public void Sleep(int ms){
		try {
			System.out.println("Sleeping...");
			Thread.sleep(ms);
			System.out.println("Done sleeping!");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void TweetSetTriggerToWordCount(){
		String setName = "TweetSet";
		Sysmap wordCountSysmap = this.getSysmap("TweetWordCountSet");
		System.out.println("$ "+wordCountSysmap);
		Trigger trigger = new TweetSetTriggerToWordCount(wordCountSysmap, waitTime1, minLoad1);
		this.setTrigger(setName, trigger);
	}
	
	public void TotalWordsCountTrigger(){
		String wcSetName = "TweetWordCountSet";
		Sysmap totalSysmap = this.getSysmap("TotalWordsCountSet");
		System.out.println("$ "+totalSysmap);
		Trigger trigger2 = new TotalWordsCountTrigger(totalSysmap, waitTime2, minLoad2);
		this.setTrigger(wcSetName, trigger2);
	}
	
	//this class will submit Sets and Triggers to Titan
	public static void main(String[] args) {
		//Connect to the DHT...
		ResourcesSetup rs = new ResourcesSetup();
		if((args.length==6)){
			rs.numberOfPartitions = new Integer(args[0]);
			rs.wcNumberOfPartitions = new Integer(args[1]);
			rs.waitTime1 = new Integer(args[2]);
			rs.minLoad1 = new Integer(args[3]);
			rs.waitTime2 = new Integer(args[4]);
			rs.minLoad2 = new Integer(args[5]);
		}else if(args.length!=0){
			System.err.println("Incorrect usage: ResourcesSetup TweetPartitions WordsPartitions tweetsToWordWaitTime tweetsToWordMinLoad WordsToCountWaitTime WordsToCountMinLoad");
			System.exit(0);
		}

		rs.TweetSet();
		rs.WordCountSet();
		rs.TotalWordCountSet();
//		
		rs.Sleep(10000);
//		
		rs.TweetSetTriggerToWordCount();
		rs.TotalWordsCountTrigger();
	}
	
	private class RSHandler extends  SysHandler.ReplyHandler{

		@Override
		public void onReceive(SysMessageReply reply) {
			System.out.println(console+reply.messageType);
			try{
				if(reply.messageType.equalsIgnoreCase("sysmapReply")){
					SysmapReplyMessage msg = (SysmapReplyMessage) reply;
					completeSysmapQueue.put(msg.getSysmap());
				}
			}catch(InterruptedException e){
				// TODO: error handling
				e.printStackTrace();
			}
		}

		@Override
		public void onReceive(SysmapCreateReply reply) {
			try {
				System.out.println("Sysmap creation complete: "+reply.getSysmap());
				sysmapQueue.put(reply.getSysmap());
			} catch (InterruptedException e) {
				// TODO: error handling
				e.printStackTrace();
			}
		}

		@Override
		public void onReceive(SetCreateReply reply) {
			//TODO
		}

		@Override
		public void onReceive(RpcReply reply) {
			// TODO Auto-generated method stub
			
		}
		
		
	}
	
}
