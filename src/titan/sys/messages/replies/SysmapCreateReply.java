package titan.sys.messages.replies;

import sys.dht.api.DHT;
import titan.sys.SysHandler;
import titan.sys.data.Sysmap;

public class SysmapCreateReply implements DHT.Reply{

	protected Sysmap sysmap;
	
	public SysmapCreateReply() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapCreateReply(Sysmap sysmap){
		this.sysmap = sysmap;
	}
	
	public Sysmap getSysmap() {
		return this.sysmap;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((SysHandler.ReplyHandler) handler).onReceive(conn, this);
        else
            ((SysHandler.ReplyHandler) handler).onReceive(this);
    }
}
