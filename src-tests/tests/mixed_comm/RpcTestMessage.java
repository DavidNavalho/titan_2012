package tests.mixed_comm;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class RpcTestMessage implements RpcMessage {
	
	public String message;

	public RpcTestMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public RpcTestMessage(String message) {
		this.message = message;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
//		((TitanHandler) handler).onReceive(handle, this);
	}

}
