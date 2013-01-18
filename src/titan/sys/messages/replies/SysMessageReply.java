package titan.sys.messages.replies;

import sys.dht.api.DHT;
import titan.sys.SysHandler;

public class SysMessageReply implements DHT.Reply{
	
	public String messageType;

	 /**
     * Needed for Kryo serialization
     */
    public SysMessageReply() {
    }
    
    public SysMessageReply(String messageType){
    	this.messageType = messageType;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((SysHandler.ReplyHandler) handler).onReceive(conn, this);
        else
            ((SysHandler.ReplyHandler) handler).onReceive(this);
    }

    
    
}
