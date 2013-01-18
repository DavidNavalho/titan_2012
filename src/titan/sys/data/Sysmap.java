package titan.sys.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import sys.net.api.Endpoint;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;

public class Sysmap {
	protected SetFactory setCreator;
	protected PartitionKeyFactory keyMaker;
	protected int nPartitions;
	protected String setName;
	
	protected Map<Long,Endpoint> partitions;

	public Sysmap() {
		// TODO Auto-generated constructor stub
	}
	
	public Sysmap(String setName, int nPartitions, SetFactory setCreator, PartitionKeyFactory keyMaker, HashMap<Long, Endpoint> partitions){
		this.nPartitions = nPartitions;
		this.setCreator = setCreator;
		this.keyMaker = keyMaker;
		this.setName = setName;
		this.partitions = partitions;
	}
	
	public Sysmap(String setName, int nPartitions, SetFactory setCreator, PartitionKeyFactory keyMaker){
		this.nPartitions = nPartitions;
		this.setCreator = setCreator;
		this.keyMaker = keyMaker;
		this.setName = setName;
		this.partitions = new HashMap<Long, Endpoint>();
	}
	
	public void addSysmapInformation(Long key, Endpoint endpoint){
		this.partitions.put(key, endpoint);
	}
	
	public boolean sysmapComplete(){
		if(this.nPartitions==this.partitions.keySet().size())
			return true;
		return false;
	}
	
	public PartitionKeyFactory getKeyMaker() {
		return keyMaker;
	}
	
	public SetFactory getSetCreator() {
		return setCreator;
	}
	
	public int partitions(){
		return this.nPartitions;
	}
	
	public Long getPartitionKey(SysKey key){
		return this.keyMaker.getPartitionKey(key, this.setName, this.nPartitions);
	}
	
	public Endpoint getPartition(SysKey key){
		Long partitionKey = this.keyMaker.getPartitionKey(key, this.setName, this.nPartitions);
		return this.getPartition(partitionKey);
	}
	
	public Endpoint getPartition(Long partitionKey){
		return this.partitions.get(partitionKey);
	}
	
	public Long generatePartitionKey(int partitionN){
		return this.keyMaker.getPartitionKey(partitionN, setName, nPartitions);
	}
	
	public String getSetName() {
		return setName;
	}
	
	public Map<Long, Endpoint> getPartitions() {
		return partitions;
	}
	
	@Override
	public String toString() {
		String sysmapString = "";
		Set<Entry<Long,Endpoint>> partitions = this.partitions.entrySet();
		for (Entry<Long, Endpoint> entry : partitions) {
			Endpoint endpoint = entry.getValue();
			Long key = entry.getKey();
			sysmapString+="[["+endpoint.toString()+"]."+key+"] ";
		}
		return sysmapString;
	}
	
}
