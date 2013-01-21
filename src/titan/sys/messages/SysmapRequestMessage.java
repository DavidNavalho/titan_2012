package titan.sys.messages;

import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.Key;
import sys.dht.api.DHT.MessageHandler;
import titan.sys.SysHandler;

public class SysmapRequestMessage implements DHT.Message {

	protected String messageType;
	private String setName;
	
	public SysmapRequestMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapRequestMessage(String messageType, String setName) {
		this.setName = setName;
		this.messageType = messageType;
	}
	
	public String getSetName() {
		return setName;
	}

	public String getMessageType() {
		return messageType;
	}
	
	@Override
	public void deliverTo(Handle conn, Key key, MessageHandler handler) {
		 ((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
	}
}
