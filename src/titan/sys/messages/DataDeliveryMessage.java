package titan.sys.messages;


public class DataDeliveryMessage extends SysMessage {

	private Object data;
	private Long partitionKey;
	
	public DataDeliveryMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public DataDeliveryMessage(String messageType, Object data, Long partitionKey) {
		super(messageType);
		this.data = data;
		this.partitionKey = partitionKey;
	}
	
	public Object getData() {
		return data;
	}
	
	public Long getPartitionKey() {
		return partitionKey;
	}
}
