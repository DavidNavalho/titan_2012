package utils.concurrency;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import titan.data.Data;
import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;


public class ParallelDataManager {

	protected Sysmap sysmap;
	protected Map<Long, PartitionManager> partitions;
	
	
	public ParallelDataManager() {
		// TODO Auto-generated constructor stub
	}
	
	public ParallelDataManager(Sysmap sysmap, long maxWaitTime, int minimumLoad) {
		this.sysmap = sysmap;
		this.partitions = new HashMap<Long, PartitionManager>();
		this.readyPartitions(maxWaitTime, minimumLoad);
	}
	
	private void readyPartitions(long maxWaitTime, int minimumLoad){
		for(int i=1;i<=this.sysmap.partitions();i++){
			Long partitionID = this.sysmap.getKeyMaker().getPartitionKey(i, this.sysmap.getSetName(), this.sysmap.partitions());
			PartitionManager partition = new PartitionManager(this.sysmap.getSetCreator().createEmpty(), maxWaitTime, minimumLoad, this.sysmap.getPartition(partitionID), partitionID);
			this.partitions.put(partitionID, partition);
			new DataReader(partition.data, partition, minimumLoad);
		}
	}
	
	public void addData(Object data, SysKey key){
		Long partitionKey = sysmap.getKeyMaker().getPartitionKey(key, sysmap.getSetName(), sysmap.partitions());
		PartitionManager partition = this.partitions.get(partitionKey);
		partition.addData(data, key);
	}
	
	
	private class DataReader implements Runnable{
		protected BlockingQueue<Data> queue;
		protected PartitionManager partitionManager;
		protected int loadCounter, minimumLoad;
		
		@SuppressWarnings("unused")
		public DataReader() {
			// TODO Auto-generated constructor stub
		}
		
		public DataReader(BlockingQueue<Data> data, PartitionManager partitionManager, int minimumLoad) {
			this.queue = data;
			this.partitionManager = partitionManager;
			this.minimumLoad = minimumLoad;
			this.loadCounter = 0;
			new Thread(this).start();
		}
		
		@Override
		public void run() {
			Data data;
			while(true){
				try {
					data = this.queue.take();
					int added = this.partitionManager.addData(data.data);
					this.loadCounter+=added;
					if(this.loadCounter>=this.minimumLoad){
						this.loadCounter = 0;
						this.partitionManager.syncAndDiscard();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
