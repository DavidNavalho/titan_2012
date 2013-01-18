package titan.sys;

import static sys.Sys.Sys;
import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.RpcServices;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.Key;
import sys.dht.catadupa.Node;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;
import titan.sys.data.PartitionKey;
import titan.sys.data.SysSet;
import titan.sys.data.Sysmap;
import titan.sys.data.triggers.Trigger;
import titan.sys.handlers.TitanRpcHandler;
import titan.sys.messages.SetCreation;
import titan.sys.messages.SysMessage;
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
import utils.danger.VersionControl;
//import static sys.utils.Log.Log;

public class SysNode extends DHT_Node{
	//TODO: there is no user control...

	private SysNodeHandler requestHandler;
	private SysNodeReplyHandler replyHandler;

	private PartitionNodeHandler sysPartitions;
	
	private SysmapHandler sysmaps;
	
	private RpcEndpoint rpc;
	private ServerHandler serverHandler;
	
	private DHT stub;
	
	private Map<String,BlockingQueue<SetCreateReply>> setCreators;
	
//	private boolean isMain = false;
	
	public SysNode(/*boolean isMain*/) {
		this.requestHandler = new SysNodeHandler();
		this.replyHandler = new SysNodeReplyHandler();
		this.sysPartitions = new PartitionNodeHandler();
		this.sysmaps = new SysmapHandler();
		this.setCreators = new HashMap<String, BlockingQueue<SetCreateReply>>();
//		this.isMain = isMain;
	}
	
	public void initialize(){
//		Log.setLevel(Level.ALL);
		DHT_Node.setHandler(this.requestHandler);
		this.stub = Sys.getDHT_ClientStub();
		this.rpc = rpcFactory.toService(RpcServices.TITAN.ordinal(), this.serverHandler);
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
	
	//TODO: I still must define how partitioning is done - manually for now...
	public Sysmap addSet(String setName, int nPartitions, SetFactory setCreator, PartitionKeyFactory keyMaker){
		if(!this.sysmaps.contains(setName)){//TODO: no else?
			//create the sysmap (empty for now)
			Sysmap sysmap = new Sysmap(setName, nPartitions, setCreator, keyMaker);
			//first, search for the nodes responsible for the several partitions...
			this.setCreators.put(setName, new LinkedBlockingQueue<SetCreateReply>());
			//populate a map with it....
			try{
				for(int i=1;i<=nPartitions;i++){
					Long partitionKey = keyMaker.getPartitionKey(i,setName,nPartitions);
					System.err.println("SysMain> ["+i+"] sending set creation to partition: "+setName+"."+partitionKey);
					SetCreation request = new SetCreation(setName, partitionKey, setCreator);
					//TODO: blabla
					this.stub.send(new PartitionKey(setName, partitionKey, setCreator), request, this.replyHandler);
					BlockingQueue<SetCreateReply> queue = this.setCreators.get(setName);
					SetCreateReply reply = queue.take();
					//add the partition to the sysmap
					sysmap.addSysmapInformation(reply.getPartitionKey(), reply.getEndpoint());
				}
				//and finally, add the sysmap to the Node's sysmaps map
				this.sysmaps.addSysmap(sysmap, setName);
				return sysmap;
			}catch(InterruptedException e){
				e.printStackTrace();//TODO:exceptions handling...
			}
/*			Sysmap sysmap = new Sysmap(setName, nPartitions, setCreator, keyMaker);
			BlockingQueue<SetCreateReply> queue = this.setCreators.get(setName);
			System.out.println(console+"Waiting for Partitions creation...");
			try{
				for(int i=1;i<=nPartitions;i++){
					System.out.println(console+" Partition created: "+setName+"_"+i);
					SetCreateReply reply = queue.take();
					sysmap.addSysmapInformation(reply.getPartitionKey(), reply.getEndpoint());
				}
			}catch(InterruptedException e){
				e.printStackTrace();//TODO:exceptions handling...
			}
			
			
			//and create the sysmap
//			Sysmap sysmap = new Sysmap(setName, nPartitions, setCreator, keyMaker, map);
			this.sysmaps.addSysmap(sysmap, setName);*/
			
			
//			LinkedList<SysKey> setPartitions = new LinkedList<SysKey>();
//			for(int i=0;i<nPartitions;i++){
//				SysKey partitionKey = new SysKey(setName+"_"+i);
//				setPartitions.add(partitionKey);
//			}
//			Sysmap sysmap = new Sysmap(setPartitions, factory);
//			this.sysmaps.addSysmap(sysmap, setName);
		}else{
			System.err.println(this.console+"Request for duplicate resource sysmap creation: "+setName);
		}
		return null;
	}
	
	//TODO: must actually update the other nodes...but since I only have this one right now...
	public void addTrigger(Trigger trigger, String setName){
		System.err.println("Received Add Trigger message");
		Sysmap sysmap = this.sysmaps.getSysmap(setName);
		Set<Entry<Long,Endpoint>> partitions = sysmap.getPartitions().entrySet();
		System.out.println(sysmap);
		for (Entry<Long, Endpoint> entry : partitions) {
			Endpoint triggerLocation = entry.getValue();
			Long partitionKey = entry.getKey();
			System.out.println(console+"Sending trigger to: "+setName+"."+partitionKey);
			rpc.send(triggerLocation, new TriggerDelivery(trigger, partitionKey), this.serverHandler);
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
		node.setNode(this.rpc, this.serverHandler);
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
	
	//TODO: maybe differentiate the message types....
	private class SysNodeHandler extends SysHandler.RequestHandler{
		@Override
		public void onReceive(DHT.Handle conn, Key key, SysMessage request) {
			String messageType = request.messageType;
			System.out.println(console+"Message request: "+messageType);
			if(messageType.trim().equalsIgnoreCase("sysmapRequest")){
				SysmapRequestMessage msg = (SysmapRequestMessage) request;
				Sysmap sysmap = getSysmap(msg.getSetName());
				conn.reply(new SysmapReplyMessage("sysmapReply",sysmap));
			}/*else if(messageType.trim().equalsIgnoreCase("addData")){
				System.err.println("!!!!");
				DataDeliveryMessage msg = (DataDeliveryMessage) request;
//				dataDelivery(msg.getData(), msg.getPartitionKey());
				if(conn.expectingReply())
					System.out.println("BLARGH");
			}else if(messageType.trim().equalsIgnoreCase("addSet")){
				DataDeliveryMessage msg = (DataDeliveryMessage) request;
				//TODO
			}else if(messageType.trim().equalsIgnoreCase("addTrigger")){
				TriggerCreationMessage msg = (TriggerCreationMessage) request;
				addTrigger(msg.getTrigger(), msg.getSetName());
			}*/
			else{
				System.err.println(console+"Message type unknown or invalid. Message ignored.");
			}
		}
		
		@Override
		public void onReceive(Handle con, Key key, TriggerCreationMessage msg) {
			System.err.println("Sys> Trigger Creation msg for "+msg.getSetName());
			VersionControl vc = msg.getVc();
			synchronized (control) {
				if(control.containsKey(vc.getId())){
					VersionControl previous = control.get(vc.getId());
					if(previous.getVersion()>=vc.getVersion()){
						System.err.println("Duplicate message! Ignoring...");
						con.reply(new SysMessageReply("trigger"));//TODO
						return;
					}
				}
				System.err.println(console+"Trigger Creation message received.");
				this.control.put(vc.getId(), vc);
			}
			addTrigger(msg.getTrigger(), msg.getSetName());
			System.err.println(console+"Triggers installed.");
			con.reply(new SysMessageReply("trigger"));
		}
		
		@Override
		public void onReceive(Handle con, Key key, SetCreation message) {
			System.err.println(console+"Set creation message received");
			partitionNodeCreation(message.getPartitionKey(), message.getSetFactory());
			con.reply(new SetCreateReply(message.getSetName(), message.getPartitionKey(), self.endpoint));
		}

		private HashMap<String, VersionControl> control = new HashMap<String, VersionControl>();
		
		@Override
		public void onReceive(Handle con, Key key, SysmapCreationMessage msg) {
			System.err.println("Sys> Sysmap Creation msg for "+msg.getSetName());
			VersionControl vc = msg.getVc();
			synchronized (control) {
				if(control.containsKey(vc.getId())){
					VersionControl previous = control.get(vc.getId());
					if(previous.getVersion()>=vc.getVersion()){
						System.err.println("Duplicate message! Ignoring...");
						return;
					}
				}
				System.out.println(console+"Sysmap Creation Message received.");
				this.control.put(vc.getId(), vc);
			}
			Sysmap sysmap = addSet(msg.getSetName(), msg.getnPartitions(), msg.getFactory(), msg.getKey());
			con.reply(new SysmapCreateReply(sysmap));
		}
	}
	
	private class SysNodeReplyHandler extends SysHandler.ReplyHandler{
		@Override
		public void onReceive(SysMessageReply reply) {
//			System.out.println("RMAH!");
			//TODO: not doing anything...yet!
		}
		
		@Override
		public void onReceive(SysmapCreateReply reply) {
			System.err.println(console+"SysmapCreateReply received on Titan Node...");
			//TODO: do I come here??
		}
		
		@Override
		public void onReceive(SetCreateReply reply) {
			System.out.println(console+"Resource partition creation request returned: "+reply.getSetName()+"."+reply.getPartitionKey());
			try{
				BlockingQueue<SetCreateReply> queue = setCreators.get(reply.getSetName());
				queue.put(reply);
			}catch(InterruptedException e){
				e.printStackTrace();//TODO: error handling...
			}
		}
	}
	
	private class ServerHandler extends TitanRpcHandler.RpcHandler{
		
		@Override
		public void onReceive(RpcHandle handle, DataDelivery msg) {
//			System.out.println(console+" RPC Message received: "+msg.getMessage());
			dataDelivery(msg.getData().getObj(), msg.getPartitionKey());
//			System.out.println(console+"Replying!");
			(handle).reply(new RpcReply(true));
		}
		
		@Override
		public void onReceive(RpcHandle handle, TriggerDelivery m) {
//			System.out.println(console+"Adding new Trigger to partition: "+m.getPartitionKey());
			addTrigger(m.getTrigger(),m.getPartitionKey());
			(handle).reply(new RpcReply(true));
		}
		
		@Override
		public void onReceive(RpcHandle handle, RpcReply msg) {
			System.out.println(console+" RPC Message received: "+msg.toString());
		}
	}
	
	
}
