package titan.run.setup;

import static sys.Sys.Sys;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.dht.api.DHT;
import titan.gateway.client.GatewayClient;
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
import utils.danger.VersionControl;
//import static sys.utils.Log.Log;

public class ResourcesSetup {
	
	protected DHT stub;
	protected String console = "ResourcesSetup$ ";
	protected SysHandler.ReplyHandler asyncHandler;
	protected BlockingQueue<Sysmap> sysmapQueue;
	protected BlockingQueue<Sysmap> completeSysmapQueue;
	protected BlockingQueue<SetCreateReply> triggerQueue;
	
	protected VersionControl vc = new VersionControl("test");

	public ResourcesSetup() {
//		Log.setLevel(Level.ALL);
		this.sysmapQueue = new LinkedBlockingQueue<Sysmap>();
		this.completeSysmapQueue = new LinkedBlockingQueue<Sysmap>();
		this.asyncHandler = new RSHandler();
		sys.Sys.init();
        this.stub = Sys.getDHT_ClientStub();
	}
	
	public Sysmap createResource(String resourceName, int numberOfPartitions, PartitionKeyFactory resourceKeyFactory, SetFactory resourceFactory){
//		try {
			System.err.println(console+"Creating resource: "+resourceName);
			SysmapCreationMessage scm = new SysmapCreationMessage("registerSet", resourceName, resourceKeyFactory, numberOfPartitions, resourceFactory);
			this.vc.inc();
			scm.setVC(this.vc);
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
		this.vc.inc();
		msg.setVC(this.vc);
		this.stub.send(new SysKey(setName), msg, this.asyncHandler);
		//TODO: resume with result...
	}
	
	//this class will submit Sets and Triggers to Titan
	public static void main(String[] args) {
		//defaults:
		//DataSets
		//TweetSet Partitions
		int numberOfPartitions = 4;
		//WordsSet Partitions
		int wcNumberOfPartitions = 2;
		//Final Count Partition -> must be one, so no need to add an argument to it...
		int totalNumberOfPartitions = 1;
		//Triggers:
		//TweetSet to TweetWordCountSet Trigger
		int waitTime1 = 50;//in ms
		int minLoad1 = 100;
		//TweetWordCountSet to TotalWordsCountSet Trigger
		int waitTime2 = 50;//in ms
		int minLoad2 = 10;
		//done
		if((args.length==6)){
			numberOfPartitions = new Integer(args[0]);
			wcNumberOfPartitions = new Integer(args[1]);
			waitTime1 = new Integer(args[2]);
			minLoad1 = new Integer(args[3]);
			waitTime2 = new Integer(args[4]);
			minLoad2 = new Integer(args[5]);
		}else if(args.length!=0){
			System.err.println("Incorrect usage: ResourcesSetup TweetPartitions WordsPartitions tweetsToWordWaitTime tweetsToWordMinLoad WordsToCountWaitTime WordsToCountMinLoad");
			System.exit(0);
		}
		//Connect to the DHT...
		ResourcesSetup rs = new ResourcesSetup();
		//Create the first Resource: a TweetSet
		//requires: name, #ofPartitions, resourceFactory
		String setName = "TweetSet";
		TweetSetKeyFactory setKey = new TweetSetKeyFactory();
		SetFactory factory = new TweetSetFactory();
		/*Sysmap tweetSetSysmap =*/ rs.createResource(setName, numberOfPartitions, setKey, factory);
		//create WordCountSet
		String wcSetName = "TweetWordCountSet";
		TweetWCSetKeyFactory wcSetKey = new TweetWCSetKeyFactory();
		SetFactory wcFactory = new WCTweetSetFactory();
		/*Sysmap wordCountSysmap = */rs.createResource(wcSetName, wcNumberOfPartitions, wcSetKey, wcFactory);
		//create TotalWordCountSet
		String totalSetName = "TotalWordsCountSet";
		TotalWordsSetKeyFactory totalSetKey = new TotalWordsSetKeyFactory();
		SetFactory totalFactory = new TotalSetFactory();
		/*Sysmap totalSysmap = */rs.createResource(totalSetName, totalNumberOfPartitions, totalSetKey, totalFactory);
		try {
			System.out.println("Sleeping...");
			Thread.sleep(10000);
			System.out.println("Done sleeping!");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//Doing a TimeWindow over TotalWords every 
//		GatewayClient gw = new GatewayClient();
//		Sysmap wordCountSysmap = gw.requestSysmap("TweetWordCountSet");
		Sysmap wordCountSysmap = rs.getSysmap("TweetWordCountSet");
		System.out.println("$ "+wordCountSysmap);
//		Sysmap totalSysmap = gw.requestSysmap("TotalWordsCountSet");
		Sysmap totalSysmap = rs.getSysmap("TotalWordsCountSet");
		System.out.println("$ "+totalSysmap);
		//doing it async for now....
		//Now that we have the sysmaps, we can add the triggers!
		//1st Trigger in TweetSet:
		Trigger trigger = new TweetSetTriggerToWordCount(wordCountSysmap, waitTime1, minLoad1);
		rs.setTrigger(setName, trigger);
		//2nd Trigger in TweetWordCountSet
		Trigger trigger2 = new TotalWordsCountTrigger(totalSysmap, waitTime2, minLoad2);
		rs.setTrigger(wcSetName, trigger2);
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
