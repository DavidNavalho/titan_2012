package titan.sys.messages.rpc;

import sys.dht.api.DHT;
import titan.sys.SysHandler;
import titan.sys.data.SysSet.SysData;


public class DataDelivery implements DHT.Message{
	
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
	public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
		((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
	}
	
//	@Override
//	public void deliverTo(RpcHandle handle, RpcHandler handler) {
//		((TitanRpcHandler.RpcHandler) handler).onReceive(handle, this);
//	}
}
