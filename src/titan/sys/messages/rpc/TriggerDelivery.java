package titan.sys.messages.rpc;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import titan.sys.data.triggers.Trigger;
import titan.sys.handlers.TitanRpcHandler;

public class TriggerDelivery implements RpcMessage {

	protected Trigger trigger;
	protected Long partitionKey;
	
	public TriggerDelivery() {
	}
	
	public TriggerDelivery(Trigger trigger, Long partitionKey){
		this.trigger = trigger;
		this.partitionKey = partitionKey;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
	
	public Trigger getTrigger() {
		return trigger;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((TitanRpcHandler.RpcHandler) handler).onReceive(handle, this);
	}

}
