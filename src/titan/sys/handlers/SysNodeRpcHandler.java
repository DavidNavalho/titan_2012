package titan.sys.handlers;

import sys.net.api.rpc.RpcHandle;
import titan.sys.messages.SetCreation;
import titan.sys.messages.rpc.DataDelivery;
import titan.sys.messages.rpc.RpcReply;
import titan.sys.messages.rpc.TriggerDelivery;

public interface SysNodeRpcHandler {

	abstract class RpcHandler extends AbstractRpcSysHandler{
		abstract public void onReceive(RpcHandle handle, DataDelivery m);
		abstract public void onReceive(RpcHandle handle, RpcReply m);
		abstract public void onReceive(RpcHandle handle, SetCreation m);
		abstract public void onReceive(RpcHandle handle, TriggerDelivery m);
	}
}
