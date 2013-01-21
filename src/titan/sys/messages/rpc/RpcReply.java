package titan.sys.messages.rpc;

import sys.dht.api.DHT;
import titan.sys.SysHandler;

public class RpcReply implements DHT.Reply{

	protected boolean received;
	
	public RpcReply() {
		// TODO Auto-generated constructor stub
	}
	
	public RpcReply(boolean received) {
		this.received = received;
	}
	
	public boolean wasReceived(){
		return this.received;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((SysHandler.ReplyHandler) handler).onReceive(conn, this);
        else
            ((SysHandler.ReplyHandler) handler).onReceive(this);
    }
}
