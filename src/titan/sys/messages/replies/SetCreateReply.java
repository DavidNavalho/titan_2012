package titan.sys.messages.replies;

import sys.dht.api.DHT;
import sys.net.api.Endpoint;
import titan.sys.SysHandler;

public class SetCreateReply implements DHT.Reply{

	protected Endpoint endpoint;
	protected Long partitionKey;
	protected String setName;
	
	public SetCreateReply() {
		// TODO Auto-generated constructor stub
	}
	
	public SetCreateReply(String setName, Long partitionKey, Endpoint endpoint){
		this.partitionKey = partitionKey;
		this.endpoint = endpoint;
		this.setName = setName;
	}
	
	public String getSetName() {
		return setName;
	}
	
	public Endpoint getEndpoint() {
		return endpoint;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
	
	@Override
	public String toString() {
		return "["+endpoint.toString()+"]."+setName+"."+partitionKey;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((SysHandler.ReplyHandler) handler).onReceive(conn, this);
        else
            ((SysHandler.ReplyHandler) handler).onReceive(this);
    }
	 
}
