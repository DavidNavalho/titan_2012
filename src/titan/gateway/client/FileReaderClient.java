package titan.gateway.client;

import static sys.Sys.Sys;

import java.io.FileNotFoundException;
import java.text.ParseException;

import sys.dht.api.DHT;
import sys.dht.catadupa.msgs.CatadupaHandler;
import sys.utils.Threading;
import tests.mixed_comm.DataExtractor;
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
import utils.concurrency.ParallelDataManager;

public class FileReaderClient extends CatadupaHandler {
	
	public static Object lock = new Object();
	
	private int tweetsPerSync;
	private Sysmap sysmap = null;
	private DataExtractor de = null;
	private ParallelDataManager manager = null;
	
	protected String console = "Client> ";

	protected DHT stub;
	protected SysHandler.ReplyHandler asyncHandler;
	
//	protected Node self;
//	protected RpcEndpoint rpc;
//	protected RpcFactory rpcFactory;
//	protected RpcHandler syncHandler;

	public FileReaderClient(String filePath, int tweetsPerSync) throws FileNotFoundException{
		this.tweetsPerSync = tweetsPerSync;
		de = new DataExtractor(filePath);
		//Log.setLevel(Level.ALL);
		sys.Sys.init();
		this.stub = Sys.getDHT_ClientStub();
		this.asyncHandler = new ClientHandler();
		initLocalNode();
	}
	public void initLocalNode() {
//		this.syncHandler= new ClientRpcHandler(); //TODO: not really this....!
//		rpcFactory = Networking.rpcBind(0, TransportProvider.DEFAULT);
//		rpc = rpcFactory.toService(RpcServices.TITAN.ordinal(), this);
//		self = new Node(rpc.localEndpoint(), Sys.getDatacenter());
//		SeedDB.init(self);
//		rpc = Networking.rpcConnect(TransportProvider.NETTY_IO_TCP).toService(0);
	}
	
	public void requestSysmap(String setName){
		this.stub.send(new SysKey(setName), new SysmapRequestMessage("sysmapRequest", setName), this.asyncHandler);
	}
	
	public void startClient(){
		while(this.sysmap==null){
			FileReaderClient.waitForIt(100);
		}
		System.out.println(console+"Sysmap received @ "+System.currentTimeMillis());
		this.manager = new ParallelDataManager(this.sysmap, 0, 1);
//		new Thread(this.manager).start();
		//read N tweetsPerSync from file; keep file descriptor; sync & discard; wait for reply;
//		new PeriodicTask(0.0, 10.0){
//			public void run(){
//				readTweets();
//			}
//		};
		Threading.newThread(true, new Runnable() {
			
			@Override
			public void run() {
				while(true){
					readTweets();
//					Threading.synchronizedWaitOn(lock);
				}
			}
		}).start();
	}
	
	public void readTweets(){
		try{
//			synchronized (manager) {
				for(int i=0;i<this.tweetsPerSync;i++){
					Tweet tweet = de.readTweet();
					if(tweet==null) break;
					manager.addData(tweet, tweet.getTweetKey());
				}
//				manager.syncAndDiscard(this.rpc, this.syncHandler);
//				System.out.println("Data sent!");
//			}
		}catch(ParseException e){
			e.printStackTrace();
		}
	}
	
	public static void waitForIt(int ms){
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
	
	private class ClientHandler extends SysHandler.ReplyHandler{
		
		@Override
		public void onReceive(SysMessageReply reply) {
			System.out.println(console+"Message received: "+reply.messageType);
			SysmapReplyMessage msg = (SysmapReplyMessage) reply;
			sysmap = msg.getSysmap();
		}
		@Override
		public void onReceive(SysmapCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void onReceive(SetCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void onReceive(RpcReply reply) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
//	private class ClientRpcHandler extends TitanRpcHandler.RpcHandler{
//
//		@Override
//		public void onReceive(RpcHandle handle, DataDelivery m) {
//			System.out.println(console+m.getMessage());
//			//TODO should not come to this.... - maybe a sepparate RpcHandler - ClientHandler!
//		}
//
//		@Override
//		public void onReceive(RpcHandle handle, RpcReply m) {
////			System.out.println(console+m.toString());
//		}
//
//		@Override
//		public void onReceive(RpcHandle handle, TriggerDelivery m) {
//			// TODO Auto-generated method stub
//			
//		}
//		
//
//	}
	
	public static void main(String[] args) throws Exception{
		String dataLocation = "/Users/jinx/Documents/eclipse/Tinkering/test/checkin_data_1.txt";
		int tweetsToRead = 1000;
		//ready client
		if(args.length==2){
			tweetsToRead = new Integer(args[0]);
			dataLocation = args[1];
		}
		FileReaderClient client = new FileReaderClient(dataLocation, tweetsToRead);
		System.out.println("Client> Client initialized @ "+System.currentTimeMillis());
		client.requestSysmap("TweetSet");
		client.startClient();
		//request client sysmap / startClient
//		String setName = "TweetSet2";
//		TweetSetKeyFactory setKey = new TweetSetKeyFactory();
//		int numberOfPartitions = 4;
//		SetFactory factory = new TweetSetFactory();
//		SysmapCreationMessage msg = new SysmapCreationMessage("registerSet", setName, setKey, numberOfPartitions, factory);
//		Endpoint endpoint = Discovery.lookup(Catadupa.discoveryName(), 1000);
//		client.rpc.send(endpoint, msg);
		
		
	}
	
}
