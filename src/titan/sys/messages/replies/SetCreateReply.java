package titan.sys.messages.replies;

import sys.dht.api.DHT;
import titan.sys.SysHandler;

public class SetCreateReply implements DHT.Reply{

	protected int totalPartitions;
	protected Long partitionKey;
	protected String setName;
	
	public SetCreateReply() {
		// TODO Auto-generated constructor stub
	}
	
	public SetCreateReply(String setName, int totalPartitions, Long partitionKey){
		this.partitionKey = partitionKey;
		this.setName = setName;
		this.totalPartitions = totalPartitions;
	}
	
	public String getSetName() {
		return setName;
	}
	
	public int getTotalPartitions() {
		return totalPartitions;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
	
	@Override
	public String toString() {
		return setName+"["+totalPartitions+"]"+"."+partitionKey;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((SysHandler.ReplyHandler) handler).onReceive(conn, this);
        else
            ((SysHandler.ReplyHandler) handler).onReceive(this);
    }
	 
}
