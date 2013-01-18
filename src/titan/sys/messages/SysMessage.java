package titan.sys.messages;

import sys.dht.api.DHT;
import titan.sys.SysHandler;

public class SysMessage implements DHT.Message{

    public String messageType;

    /**
     * Needed for Kryo serialization
     */
    public SysMessage() {
    }

    public SysMessage(String messageType) {
        this.messageType = messageType;
    }
    
    public void setMessageType(String messageType){
    	this.messageType = messageType;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
    }
	
}
