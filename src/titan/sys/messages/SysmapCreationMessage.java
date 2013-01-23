package titan.sys.messages;

import sys.dht.api.DHT;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;
import titan.sys.SysHandler;

public class SysmapCreationMessage implements DHT.Message{
	
	protected int nPartitions;
	protected PartitionKeyFactory setKey;
	protected SetFactory factory;
	protected String setName;
	
	public SysmapCreationMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapCreationMessage(String messageType, String setName, PartitionKeyFactory setKey, int numberOfPartitions, SetFactory factory){
		this.nPartitions = numberOfPartitions;
		this.setKey = setKey;
		this.factory = factory;
		this.setName = setName;
	}
	
	public int getnPartitions() {
		return nPartitions;
	}
	
	public String getSetName() {
		return setName;
	}
	
	public PartitionKeyFactory getKey() {
		return setKey;
	}
	
	public SetFactory getFactory() {
		return factory;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
    }
}
