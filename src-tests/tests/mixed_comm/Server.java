package tests.mixed_comm;

import static sys.Sys.Sys;
import static sys.net.api.Networking.Networking;
import sys.RpcServices;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.catadupa.Node;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import titan.sys.SysHandler;
import titan.sys.handlers.TitanRpcHandler;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.rpc.DataDelivery;
import titan.sys.messages.rpc.RpcReply;
import titan.sys.messages.rpc.TriggerDelivery;


public class Server {
	
	protected Node self;
	protected RpcEndpoint rpc;
	protected RpcFactory rpcFactory;
	private DHT stub;
	private SysNodeReplyHandler replyHandler;
	private ServerHandler serverHandler;
	
	public Server() {
		this.replyHandler = new SysNodeReplyHandler();
		
		sys.Sys.init();
		DHT_Node.start();
		rpcFactory = Networking.rpcBind(0, TransportProvider.DEFAULT);
		this.serverHandler = new ServerHandler();
		rpc = rpcFactory.toService(RpcServices.TITAN.ordinal(), this.serverHandler);
		self = new Node(rpc.localEndpoint(), Sys.getDatacenter());
//		SeedDB.init(self);
		this.stub = Sys.getDHT_ClientStub();
	}
	
	
	private class SysNodeReplyHandler extends SysHandler.ReplyHandler{
		@Override
		public void onReceive(SysMessageReply reply) {
//			System.out.println("RMAH!");
			//TODO: not doing anything...yet!
		}

		@Override
		public void onReceive(SysmapCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
		
		@Override
		public void onReceive(SetCreateReply reply) {
			// TODO Auto-generated method stub
			
		}
	}
	
	private class ServerHandler extends TitanRpcHandler.RpcHandler{
		
		@Override
		public void onReceive(RpcHandle handle, DataDelivery msg) {
//			System.out.println(console+" RPC Message received: "+msg.getMessage());
//			dataDelivery(msg.getData(), msg.getPartitionKey());
//			System.out.println(console+"Replying!");
			(handle).reply(new RpcReply(true));
		}
		
		
		
		@Override
		public void onReceive(RpcHandle handle, TriggerDelivery m) {
			
		}
		
		@Override
		public void onReceive(RpcHandle handle, RpcReply msg) {
			System.out.println(" RPC Message received: "+msg.toString());
		}
	}
}
