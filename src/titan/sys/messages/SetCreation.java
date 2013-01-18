package titan.sys.messages;

import sys.dht.api.DHT;
import titan.gateway.setup.SetFactory;
import titan.sys.SysHandler;

public class SetCreation implements DHT.Message {
	
	protected Long partitionKey;
	protected String setName;
	protected SetFactory setFactory;
	
	public SetCreation() {
	}
	
	public SetCreation(String setName, Long partitionKey, SetFactory setFactory) {
		this.partitionKey = partitionKey;
		this.setName = setName;
		this.setFactory = setFactory;
	}
	
	public String getSetName() {
		return setName;
	}
	
	public SetFactory getSetFactory() {
		return setFactory;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
    }

}
