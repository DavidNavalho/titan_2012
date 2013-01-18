package tests.mixed_comm;

import sys.net.api.Endpoint;
import titan.sys.messages.replies.SysMessageReply;

public class ServerSysMessage extends SysMessageReply {

	public Endpoint serverRpcEndpoint;
	
	public ServerSysMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public ServerSysMessage(String message, Endpoint serverRpcEndpoint) {
		this.messageType = message;
		this.serverRpcEndpoint = serverRpcEndpoint;
	}
	
}
