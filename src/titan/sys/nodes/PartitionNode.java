package titan.sys.nodes;

import java.util.LinkedList;

import titan.sys.data.SysSet;
import titan.sys.data.triggers.Trigger;

public class PartitionNode{
	
	private SysSet set;
	private Long partitionKey;
	private LinkedList<Trigger> triggers;
	
	public PartitionNode(){
		//Needed for Kryo...
	}
	
	public PartitionNode(Long partitionKey, SysSet set){
		this.partitionKey = partitionKey;
		this.set = set;
		this.triggers = new LinkedList<Trigger>();
//		System.err.println("New partition created: "+this.partitionKey);
	}
	
//	public void setNode(){
//	}

	public void addData(Object data) {
		this.set.add(data);
//		System.out.println("PartitionNode> "+this.set.getPartitionName());
//		if(this.set.getPartitionName().equalsIgnoreCase("total")){
//			LinkedList<Integer> ll = (LinkedList<Integer>) this.set.getData();
//			System.out.println("Total words count: "+ll.get(0));
//		}
	}

	private int lastCounter = 0;
	private Integer floodCheck = new Integer(0);
	//TODO: for now, always process new data...only working on bulk, though...
	public int addBulkData(Object data){
//		synchronized (this.floodCheck) {
//			this.floodCheck++;
//		}
		LinkedList<Object> bulkData = (LinkedList<Object>) data;
//		
		for (Object object : bulkData) {
			this.set.add(object);
		}
		
		if(this.set.getPartitionName().equalsIgnoreCase("total")){
//			for (Object object : bulkData) {
//				this.set.add(object);
//			}
			LinkedList<Integer> ll = (LinkedList<Integer>) this.set.getData().getObj();
			int currentCount = ll.get(0);
			if((currentCount-lastCounter)>100000){
				System.err.println(""+System.currentTimeMillis()+": Total words count: "+currentCount);
				lastCounter = currentCount;
			}
		}
//		if(this.set.getPartitionName().equalsIgnoreCase("total"))
//			System.out.println("Total: "+bulkData.size()+" received.");
//		else if(this.set.getPartitionName().equalsIgnoreCase("tweetset"))
//			System.out.println("TweetSet: "+bulkData.size()+" received.");
//		else if(this.set.getPartitionName().equalsIgnoreCase("wordcountset"))
//			System.out.println("WordCountSet: "+bulkData.size()+" received.");
		
//		synchronized (this.floodCheck) {
//			this.floodCheck--;
//		}
		this.processData(bulkData);
		return bulkData.size();
	}
	
	//TODO: this is still not ok...
	private void processData(LinkedList<Object> dataToProcess){
//		System.err.println("SYSTEM TEST");
		synchronized (triggers) {
			
		
			for (Trigger trigger : triggers) {
//				System.err.println("Partition "+this.partitionKey+": Trigger ["+trigger.getTriggerName()+"] active...");
				trigger.processData(dataToProcess);
//				trigger.syncData(stub);
				trigger.syncData();
			}
		}
	}
	
	public void addTrigger(Trigger trigger){
		trigger.setManager();
		this.triggers.add(trigger);
	}

	public void mergeSet(SysSet mergeableSet) {
		this.set.merge(mergeableSet);
	}

	public Long getKey() {
		return this.partitionKey;
	}

	public SysSet getSet() {
		return this.set;
	}
	
	

}
