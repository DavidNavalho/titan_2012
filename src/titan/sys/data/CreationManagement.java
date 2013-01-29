package titan.sys.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//TODO: I think I only need the completeSets....
public class CreationManagement {

	protected List<DataControl> dataControl;//TODO: I can do away with this....
	protected Map<String,BlockingQueue<String>> completeSets;
	
	protected class DataControl{
		protected String name;
		protected int partition;
		protected long partitionID;
		public DataControl(String name, int partition, long partitionID) {
			this.name = name;
			this.partition = partition;
			this.partitionID = partitionID;
		}
	}
	
	//BlockingQueue
	public CreationManagement(){
		this.dataControl = new LinkedList<DataControl>();
		this.completeSets = new HashMap<String,BlockingQueue<String>>();
	}
	
	private DataControl findData(String setName, int totalPartitions, long partitionKey){
		for (DataControl control : this.dataControl) {
			if(control.name.equalsIgnoreCase(setName))
				if(control.partition==totalPartitions)
					if(control.partitionID==partitionKey)
						return control;
		}
		return null;
	}
	
//	private boolean isComplete(String setName, int partitions){
//		int count = 0;
//		for (DataControl control : this.dataControl) {
//			if(control.name.equalsIgnoreCase(setName))
//				count++;
//			if(count==partitions)
//				return true;
//		}
//		return false;
//	}
	
	public synchronized boolean addPartition(String setName, int totalPartitions, long partitionKey){
		DataControl data = this.findData(setName, totalPartitions, partitionKey);
		String key = setName+"_"+partitionKey;
		if(data==null){//Then it's not already added, and it should be added
			this.dataControl.add(new DataControl(setName, totalPartitions, partitionKey));
			try {
				synchronized (completeSets) {
					if(!this.completeSets.containsKey(key))
						this.completeSets.put(key, new LinkedBlockingQueue<String>());
				}
				this.completeSets.get(key).put(key);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;//returning false means a repeat...
	}
	
	public void waitForCompletion(String setName, long partitionKey){
		String key = setName+"_"+partitionKey;
		System.out.println("Waiting for completion on: "+key);
		BlockingQueue<String> waiter = null;
		synchronized (completeSets) {
			if(!this.completeSets.containsKey(key))
				this.completeSets.put(key, new LinkedBlockingQueue<String>());
			waiter = this.completeSets.get(key);
		}
		try {
			waiter.take();//TODO: this only works if it enters this block once...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
