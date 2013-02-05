package titan.sys;

import static sys.Sys.Sys;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.Key;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;
import titan.sys.data.CreationManagement;
import titan.sys.data.SysKey;
import titan.sys.data.SysSet;
import titan.sys.data.Sysmap;
import titan.sys.data.triggers.Trigger;
import titan.sys.messages.SetCreation;
import titan.sys.messages.SysmapCreationMessage;
import titan.sys.messages.SysmapRequestMessage;
import titan.sys.messages.TriggerCreationMessage;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.replies.SysmapReplyMessage;
import titan.sys.messages.rpc.DataDelivery;
import titan.sys.messages.rpc.RpcReply;
import titan.sys.messages.rpc.TriggerDelivery;
import titan.sys.nodes.PartitionNode;
import titan.sys.nodes.PartitionNodeHandler;
import titan.sys.nodes.SysmapHandler;
//import utils.danger.VersionControl;
//import static sys.utils.Log.Log;

public class SysNode extends DHT_Node{
	//TODO: there is no user control...

	private SysNodeHandler requestHandler;
	private SysNodeReplyHandler replyHandler;

	private PartitionNodeHandler sysPartitions;
	
	private SysmapHandler sysmaps;
	
//	private RpcEndpoint rpc;
//	private ServerHandler serverHandler;
	
	private DHT stub;
	private SysNodeDataHandler dataHandler;
//	private boolean isMain = false;
	
	public SysNode(/*boolean isMain*/) {
		this.requestHandler = new SysNodeHandler();
		this.replyHandler = new SysNodeReplyHandler();
		this.sysPartitions = new PartitionNodeHandler();
		this.sysmaps = new SysmapHandler();
		this.dataHandler = new SysNodeDataHandler();
		new Thread(this.dataHandler).start();
//		this.isMain = isMain;
	}
	
	public void initialize(){
//		Log.setLevel(Level.ALL);
		DHT_Node.setHandler(this.requestHandler);
		this.stub = Sys.getDHT_ClientStub();
//		this.rpc = rpcFactory.toService(RpcServices.TITAN.ordinal(), this.serverHandler);
		System.err.println(console+"System initialized.");
		System.err.println("Key: "+self.key+", endpoint:"+self.endpoint+", DBkeys: "+db.nodeKeys());
		//TODO: how do i initialize the reply handler??? - for the tests, I'll keep 1 node for now...
//		DHT_Node.setHandler(this.replyHandler);
	}
	
//	private void initLocalNode(){
//		rpcFactory = Networking.rpcBind(0, TransportProvider.DEFAULT);
//		this.serverHandler = new ServerHandler();
//		
////		self = new Node(rpc.localEndpoint(), Sys.getDatacenter());
//		
//		
//
//	}
	
	protected HashMap<String, String> sysmapsControl = new HashMap<String, String>();
	protected CreationManagement control = new CreationManagement();
	
	//TODO: I am returning a sysmap when the creation process may not yet be finished...(due to repeated messages - this can lead to problems later)
	public Sysmap addSet(SysmapCreationMessage msg){//String setName, int nPartitions, SetFactory setCreator, PartitionKeyFactory keyMaker){
		String setName = msg.getSetName();
		int nPartitions = msg.getnPartitions();
		SetFactory setCreator = msg.getFactory();
		PartitionKeyFactory keyMaker = msg.getKey();
		Sysmap sysmap = new Sysmap(setName, nPartitions, setCreator, keyMaker);
		synchronized (this.sysmapsControl) {
			System.out.println(this.console+"New sysmap creation message received for "+setName);
			if(this.sysmapsControl.containsKey(setName)){
				System.out.println(this.console+"Repeated request for sysmap creation ("+setName+")");
				return sysmap;
			}
			else{
				System.out.println("Adding set to sysmapsControl: "+setName);
				this.sysmapsControl.put(setName, setName);
			}
		}
		//Partitions need to be created on the several nodes...; and the according sysmap created(done) and returned
		for(int i=1;i<=nPartitions;i++){
			Long partitionKey = keyMaker.getPartitionKey(i, setName, nPartitions);
			//put it on the controller before sending the request!
			boolean controllerAnswer = this.control.addPartition(setName, nPartitions, partitionKey);
			if(!controllerAnswer)
				System.out.println(console+"Attempted to add a repeated partition?");
			System.out.println("Sending request for partition creation: "+setName+"["+partitionKey+"]");
			SetCreation request = new SetCreation(setName, partitionKey, setCreator, nPartitions);
			this.stub.send(new SysKey(partitionKey),request,this.replyHandler);
			System.out.println("Waiting for request completion...");
			this.control.waitForCompletion(setName, partitionKey);
			System.out.println("partition creation succeded");
		}
		//since sets are created, we can complete with the sysmap creation...
		this.sysmaps.addSysmap(sysmap, setName);
		System.out.println("Sysmap created successfully!");
		return sysmap;
	}
	
	//TODO: must actually update the other nodes...but since I only have this one right now...
	public void addTrigger(Trigger trigger, String setName){
		System.err.println("Received Add Trigger message");
		Sysmap sysmap = this.sysmaps.getSysmap(setName);
		Set<Entry<Long,DHT>> partitions = sysmap.getPartitions().entrySet();
		System.out.println(sysmap);
		for (Entry<Long, DHT> entry : partitions) {
			DHT triggerLocation = entry.getValue();
			Long partitionKey = entry.getKey();
			System.out.println(console+"Sending trigger to: "+setName+"."+partitionKey);
			triggerLocation.send(new SysKey(partitionKey), new TriggerDelivery(trigger, partitionKey), this.replyHandler);
//			rpc.send(triggerLocation, new TriggerDelivery(trigger, partitionKey), this.serverHandler);
			//I'll wait for the answers...
			System.out.println(console+"Trigger sent.");
		}
		
		/*PartitionKeyFactory keyFactory = sysmap.getKeyMaker();
		for(int i=1;i<=sysmap.partitions();i++){
			Long partitionKey = keyFactory.getPartitionKey(i, setName, sysmap.partitions());
			Endpoint triggerLocation = sysmap.getPartition(partitionKey);
			System.out.println(console+"Sending trigger to: "+setName+"."+partitionKey);
			rpc.send(triggerLocation, new TriggerDelivery(trigger, partitionKey), this.serverHandler);
			//I'll wait for the answers...
			System.out.println(console+"Trigger sent.");
//			this.sysPartitions.addTrigger(partitionKey, trigger);
//			System.err.println(this.console+"Trigger ["+trigger.getTriggerName()+"] successfully added to partition: "+partitionKey);
		}*/
	}
	
	//TODO:use queue so I can identify when triggers have been delivered
	public void addTrigger(Trigger trigger, Long partitionKey){
		this.sysPartitions.addTrigger(partitionKey, trigger);
	}
	
	public Sysmap getSysmap(String mapKey){
//		System.err.println("Sysmap request(mapKey): "+mapKey);
		return this.sysmaps.getSysmap(mapKey);
	}
	
	public void addPartitionNode(PartitionNode node){
		this.sysPartitions.addNode(node);
	}

	public void partitionNodeCreation(Long partitionKey, SetFactory creator){
		PartitionNode node = new PartitionNode(partitionKey, creator.createEmpty());
		this.addPartitionNode(node);
	}
	
	//TODO: PartitionNode node = new PartitionNode(partitionKey, setCreator.createEmpty());
	//TODO: Send message
	//TODO:	this.addPartitionNode(node);
	
	public void dataDelivery(Object data, Long partitionKey){
		if(this.sysPartitions.exists(partitionKey)){
//			LinkedList<Object> dataList = (LinkedList<Object>) data;
//			for (Object object : dataList) {
//				this.sysPartitions.addMessage(partitionKey, object);
//			}
//			DHT stub = Sys.getDHT_ClientStub();
			int parsed = this.sysPartitions.addData(partitionKey, data);
//			System.err.println(this.console+"["+parsed+"] Data successfully added on partition: "+partitionKey);
//			this.sysPartitions.addData(partitionKey, data);
		}else{
			//TODO: create partition based on sysmap rules - fetch sysmap first! - for that I need the actual key, not just a String
			//TODO: fetch sysmap from other nodes if need be!
			//TODO: I'll just use 1 server for now, complete this later...
//			Sysmap sysmap = this.getSysmap(key)
			System.err.println("Data Delivery failed!");
		}
	}
	
	public void setDelivery(Object data, Long partitionKey){
		if(this.sysPartitions.exists(partitionKey)){
			SysSet set = (SysSet) data;
			this.sysPartitions.mergeData(partitionKey, set);
//			System.err.println(this.console+"Data successfully merged on partition: "+partitionKey);
//			this.sysPartitions.addData(partitionKey, data);
		}else{
			//TODO: create partition based on sysmap rules - fetch sysmap first! - for that I need the actual key, not just a String
			//TODO: fetch sysmap from other nodes if need be!
			//TODO: I'll just use 1 server for now, complete this later...
//			Sysmap sysmap = this.getSysmap(key)
		}
	}
	
	private String console = "Sys> ";
	
	protected BlockingQueue<DataDelivery> dataQueue;
	
	private class SysNodeDataHandler implements Runnable{
		
		
		
		public SysNodeDataHandler() {
			dataQueue = new LinkedBlockingQueue<DataDelivery>();
		}
		
		@Override
		public void run() {
			while(true){
				try {
					DataDelivery dataMsg = dataQueue.take();
					dataDelivery(dataMsg.getData().getObj(), dataMsg.getPartitionKey());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//TODO: maybe differentiate the message types....
	private class SysNodeHandler extends SysHandler.RequestHandler{
		
//		@Override
//		public void onReceive(DHT.Handle conn, Key key, SysMessage request) {
//			String messageType = request.messageType;
//			System.out.println(console+"Message request: "+messageType);
//			if(messageType.trim().equalsIgnoreCase("sysmapRequest")){
//				SysmapRequestMessage msg = (SysmapRequestMessage) request;
//				Sysmap sysmap = getSysmap(msg.getSetName());
//				conn.reply(new SysmapReplyMessage("sysmapReply",sysmap));
//			}/*else if(messageType.trim().equalsIgnoreCase("addData")){
//				System.err.println("!!!!");
//				DataDeliveryMessage msg = (DataDeliveryMessage) request;
////				dataDelivery(msg.getData(), msg.getPartitionKey());
//				if(conn.expectingReply())
//					System.out.println("BLARGH");
//			}else if(messageType.trim().equalsIgnoreCase("addSet")){
//				DataDeliveryMessage msg = (DataDeliveryMessage) request;
//				//TODO
//			}else if(messageType.trim().equalsIgnoreCase("addTrigger")){
//				TriggerCreationMessage msg = (TriggerCreationMessage) request;
//				addTrigger(msg.getTrigger(), msg.getSetName());
//			}*/
//			else{
//				System.err.println(console+"Message type unknown or invalid. Message ignored.");
//			}
//		}
		
		@Override
		public void onReceive(Handle con, Key key, SysmapRequestMessage message) {
			System.out.println("Sys> Sysmap request message received");
			Sysmap sysmap = getSysmap(message.getSetName());
			con.reply(new SysmapReplyMessage("sysmapReply", sysmap));
		}
		
		@Override
		public void onReceive(Handle con, Key key, TriggerCreationMessage msg) {
			System.err.println("Sys> Trigger Creation msg for "+msg.getSetName());
//			VersionControl vc = msg.getVc();
//			synchronized (control) {
//				if(control.containsKey(vc.getId())){
//					VersionControl previous = control.get(vc.getId());
//					if(previous.getVersion()>=vc.getVersion()){
//						System.err.println("Duplicate message! Ignoring...");
//						con.reply(new SysMessageReply("trigger"));//TODO
//						return;
//					}
//				}
//				System.err.println(console+"Trigger Creation message received.");
//				this.control.put(vc.getId(), vc);
//			}
			addTrigger(msg.getTrigger(), msg.getSetName());
			System.err.println(console+"Triggers installed.");
			con.reply(new SysMessageReply("trigger"));
		}
		
		@Override
		public void onReceive(Handle con, Key key, SetCreation message) {
			System.out.println(console+"Set creation message received: "+message.getSetName()+"|"+message.getPartitionKey());
			partitionNodeCreation(message.getPartitionKey(), message.getSetFactory());
			System.out.println("Partition created: "+message.getSetName()+"|"+message.getPartitionKey());
			con.reply(new SetCreateReply(message.getSetName(), message.getTotalPartitions(), message.getPartitionKey()));
		}

//		private HashMap<String, VersionControl> control = new HashMap<String, VersionControl>();
		
		@Override
		public void onReceive(Handle con, Key key, SysmapCreationMessage msg) {
			System.err.println("Sys> Sysmap Creation msg for "+msg.getSetName());
			Sysmap sysmap = addSet(msg);
			con.reply(new SysmapCreateReply(sysmap));
		}

		@Override
		public void onReceive(Handle con, Key key, DataDelivery msg) {
			// TODO Auto-generated method stub
//			System.out.println("DataDelivery message received for: "+msg.getPartitionKey());
			
//			dataDelivery(msg.getData().getObj(), msg.getPartitionKey());
			try {
				dataQueue.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
//			System.out.println("Data delivered!");
//			System.out.println(console+"Replying!");
//			if(con.expectingReply())
//				con.reply(new RpcReply(true));
		}

		@Override
		public void onReceive(Handle con, Key key, TriggerDelivery m) {
			// TODO Auto-generated method stub
			System.out.println("Sys> Trigger delivery: "+m.toString());
			addTrigger(m.getTrigger(),m.getPartitionKey());
			con.reply(new RpcReply(true));
		}

	}
	
	private class SysNodeReplyHandler extends SysHandler.ReplyHandler{
		@Override
		public void onReceive(SysMessageReply reply) {
			System.out.println("RMAH!");
			//TODO: not doing anything...yet!
		}
		
		@Override
		public void onReceive(SysmapCreateReply reply) {
			System.err.println(console+"SysmapCreateReply received on Titan Node...");
			//TODO: do I come here??
		}
		
		@Override
		public void onReceive(SetCreateReply reply) {
			System.out.println(console+"Resource partition creation request returned: "+reply.toString());
			control.addPartition(reply.getSetName(), reply.getTotalPartitions(), reply.getPartitionKey());
		}

		//TODO: is this being used?
		@Override
		public void onReceive(RpcReply reply) {
			// TODO Auto-generated method stub
			System.out.println("Sys> RpcReply?");
		}
	}
	
//	private class ServerHandler extends TitanRpcHandler.RpcHandler{
//		
//		@Override
//		public void onReceive(RpcHandle handle, DataDelivery msg) {
////			System.out.println(console+" RPC Message received: "+msg.getMessage());
//			dataDelivery(msg.getData().getObj(), msg.getPartitionKey());
////			System.out.println(console+"Replying!");
//			(handle).reply(new RpcReply(true));
//		}
//		
//		@Override
//		public void onReceive(RpcHandle handle, TriggerDelivery m) {
////			System.out.println(console+"Adding new Trigger to partition: "+m.getPartitionKey());
//			addTrigger(m.getTrigger(),m.getPartitionKey());
//			(handle).reply(new RpcReply(true));
//		}
//		
//		@Override
//		public void onReceive(RpcHandle handle, RpcReply msg) {
//			System.out.println(console+" RPC Message received: "+msg.toString());
//		}
//	}
	
	
}
