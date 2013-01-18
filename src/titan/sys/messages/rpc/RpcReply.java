package titan.sys.messages.rpc;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import titan.sys.handlers.TitanRpcHandler;

public class RpcReply implements RpcMessage{

	protected boolean received;
	
	public RpcReply() {
		// TODO Auto-generated constructor stub
	}
	
	public RpcReply(boolean received){
		this.received = received;
	}
	
	public boolean wasReceived() {
		return received;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((TitanRpcHandler.RpcHandler) handler).onReceive(handle, this);
	}
}
