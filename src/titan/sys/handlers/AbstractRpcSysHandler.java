package titan.sys.handlers;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

abstract class AbstractRpcSysHandler implements RpcHandler{

	@Override
	public void onReceive(RpcMessage m){
	}

	@Override
	public void onReceive(RpcHandle handle, RpcMessage m){
	}

	@Override
	public void onFailure(RpcHandle handle){
	}
	
}
