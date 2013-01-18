package titan.sys.messages.rpc;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import titan.sys.data.SysSet.SysData;
import titan.sys.handlers.TitanRpcHandler;


public class DataDelivery implements RpcMessage{
	
	protected String message;
	protected SysData data;
	protected Long partitionKey;

	public DataDelivery() {
	}
	
	public DataDelivery(String messageType, SysData data, Long partitionKey) {
		this.message = messageType;
		this.data = data;
		this.partitionKey = partitionKey;
	}
	
	public SysData getData() {
		return data;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
	
	public String getMessage() {
		return message;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((TitanRpcHandler.RpcHandler) handler).onReceive(handle, this);
	}
}
