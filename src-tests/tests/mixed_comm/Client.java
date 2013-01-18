package tests.mixed_comm;

import static sys.net.api.Networking.Networking;
import sys.Sys;
import sys.dht.api.DHT;
import sys.dht.catadupa.Node;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import titan.sys.SysHandler;
import titan.sys.handlers.TitanRpcHandler;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.rpc.DataDelivery;
import titan.sys.messages.rpc.RpcReply;
import titan.sys.messages.rpc.TriggerDelivery;



public class Client {
	
	private DHT stub;
	private ClientHandler asyncHandler;
	private RpcHandler syncHandler;
	private boolean ready = false;
	
	public Client() {
		sys.Sys.init();
		this.stub = Sys.Sys.getDHT_ClientStub();
		initiateRpcServices();
//		this.stub.s
	}
	
	protected Node self;
	protected RpcEndpoint rpc;
	protected Endpoint endpoint;
	protected RpcFactory rpcFactory;
	
	private void initiateRpcServices(){
		syncHandler = new ClientRpcHandler();
		rpcFactory = Networking.rpcBind(0, TransportProvider.DEFAULT);
//		rpc = rpcFactory.toService(RpcServices.TITAN.ordinal(), this);
	}
	
	protected ServerSysMessage msg;
	
	private void startWorking(){
		while(!ready){
			Client.waitForIt(5000);
		}
		System.out.println("Client> Client dataflow started...");
		for(int i=1;i<=5;i++){
			System.out.println("Client> Sending data... ("+i+")");
			rpc.send(this.msg.serverRpcEndpoint, new RpcTestMessage("Rpc from client to server test."), syncHandler);
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
			System.out.println("Client> Reply received: "+reply.messageType);
			//assume I got a sysmap, and will now be sending data back...
			//how do i make it over time...?
			
			msg = (ServerSysMessage) reply;
			ready = true;
			
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
	
	private class ClientRpcHandler extends TitanRpcHandler.RpcHandler{

		@Override
		public void onReceive(RpcMessage m) {
			System.out.println("Client> "+((ServerSysMessage)m).messageType);
		}

		@Override
		public void onReceive(RpcHandle handle, RpcMessage m) {
			System.out.println("Client> "+((RpcTestMessage)m).message);
		}

		@Override
		public void onFailure(RpcHandle handle) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onReceive(RpcHandle handle, DataDelivery m) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onReceive(RpcHandle handle, RpcReply m) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onReceive(RpcHandle handle, TriggerDelivery m) {
			// TODO Auto-generated method stub
			
		}
		
	}
	

	public static void main(String[] args) {
		Server server = new Server();
		Client.waitForIt(10000);
		Client client = new Client();
		client.startWorking();
	}
}
