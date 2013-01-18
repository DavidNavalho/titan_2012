package titan.sys.messages;

public class SysmapRequestMessage extends SysMessage {

	private String setName;
	
	public SysmapRequestMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapRequestMessage(String messageType, String setName) {
		super(messageType);
		this.setName = setName;
	}
	
	public String getSetName() {
		return setName;
	}
}
