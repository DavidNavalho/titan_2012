package titan.sys.messages.rpc;

import sys.dht.api.DHT;
import titan.sys.SysHandler;
import titan.sys.data.triggers.Trigger;

public class TriggerDelivery implements DHT.Message {

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
	public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
		((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
	}

}
