package titan.sys.messages.replies;

import titan.sys.data.Sysmap;

public class SysmapReplyMessage extends SysMessageReply {

	private Sysmap sysmap;
	
	public SysmapReplyMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapReplyMessage(String messageType, Sysmap sysmap) {
		super(messageType);
		this.sysmap = sysmap;
	}
	
	public Sysmap getSysmap() {
		return sysmap;
	}
	
}
