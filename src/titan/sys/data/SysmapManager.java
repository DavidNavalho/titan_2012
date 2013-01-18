package titan.sys.data;

import java.util.HashMap;
import java.util.Map;

import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import titan.gateway.setup.PartitionKeyFactory;
import titan.sys.data.SysSet.SysData;
import titan.sys.messages.rpc.DataDelivery;

public class SysmapManager {
	private Sysmap sysmap;
	private Map<Long, SysSet> partitions;
	
	public SysmapManager() {
		// TODO Auto-generated constructor stub
	}
	
	public SysmapManager(Sysmap sysmap) {
		this.sysmap = sysmap;
		this.partitions = new HashMap<Long, SysSet>(2*sysmap.partitions());
		this.readyPartitions();
	}
	
	//TODO: I am assuming partitions are empty, thus creating new, empty sets
	private void readyPartitions(){
		for(int i=1;i<=this.sysmap.partitions();i++){
			Long partitionID = this.sysmap.getKeyMaker().getPartitionKey(i, this.sysmap.getSetName(), this.sysmap.partitions());
			this.partitions.put(partitionID, this.sysmap.getSetCreator().createEmpty());
		}
	}
	
	//typical usage for sysmap request/handling:
	//request a sysmap
	//receive the sysmap
	//prepare the SetManager, with the sysmap help
	//(TODO: what if I wanted a sysmap with 'populated data'?)
	//create multiple Sets
	//provide generic data input mechanisms
	
	public void addData(Object data, SysKey key){
		Long partitionKey = sysmap.getKeyMaker().getPartitionKey(key, sysmap.getSetName(), sysmap.partitions());
		synchronized (partitions) {
			SysSet partitionSet = this.partitions.get(partitionKey);
			if(partitionSet!=null && data!=null && partitionKey!=null)//TODO:wthell??
				partitionSet.add(data);
//			else//TODO: I am doing something wrong here, obviously...(I am losing data...)
//				System.out.println("Got a fluke: [partitionSet: "+(partitionSet==null)+"]Ê[data: "+(data==null)+"]Ê[partitionKey: "+(partitionKey==null)+"]");
		}
	}
	
	/**
	 * 
	 * @param rpc
	 * @param clientRpcHandler
	 * @return the number of syncRequests sent
	 */
	public void syncAndDiscard(RpcEndpoint rpc, RpcHandler clientRpcHandler){
//		int count=0;
		synchronized (partitions) {
			PartitionKeyFactory keyMaker = sysmap.getKeyMaker();
			String setName = sysmap.getSetName();
			int totalPartitions = sysmap.partitions();
			for(int i=1;i<=totalPartitions;i++){
				Long partitionKey = keyMaker.getPartitionKey(i, setName, totalPartitions);
				SysSet set = this.partitions.get(partitionKey);
				SysData data = set.getData();
				rpc.send(sysmap.getPartition(partitionKey), new DataDelivery("addData",data,partitionKey), clientRpcHandler, 50);
//				count+=data.getDataSize();
			}
			//zero out the partitions again...
			this.partitions = new HashMap<Long, SysSet>(2*sysmap.partitions());
			this.readyPartitions();
			}
//		return count;
//		Threading.synchronizedNotifyAllOn(FileReaderClient.lock);
	}

}
