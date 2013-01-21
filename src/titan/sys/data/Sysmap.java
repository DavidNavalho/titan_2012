package titan.sys.data;

import static sys.Sys.Sys;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import sys.dht.api.DHT;
import titan.gateway.setup.PartitionKeyFactory;
import titan.gateway.setup.SetFactory;

public class Sysmap{
	protected SetFactory setCreator;
	protected PartitionKeyFactory keyMaker;
	protected int nPartitions;
	protected String setName;SysKey syskey;
	
	transient protected Map<Long,DHT> partitions;

	public Sysmap() {
		// TODO Auto-generated constructor stub
	}
	
	public Sysmap(String setName, int nPartitions, SetFactory setCreator, PartitionKeyFactory keyMaker){
		this.nPartitions = nPartitions;
		this.setCreator = setCreator;
		this.keyMaker = keyMaker;
		this.setName = setName;
		this.partitions = null;
	}

	protected void makeStubs(){
		this.partitions = new HashMap<Long, DHT>();
		for(int i=1;i<=nPartitions;i++){
			this.partitions.put(keyMaker.getPartitionKey(i, this.setName, this.nPartitions), Sys.getDHT_ClientStub());
		}
	}

//	public void addSysmapInformation(Long key, DHT endpoint){
//		this.partitions.put(key, endpoint);
//	}
	
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
	
//	public Long getPartitionKey(SysKey key){
//		return this.keyMaker.getPartitionKey(key, this.setName, this.nPartitions);
//	}
//	
//	public DHT getPartition(SysKey key){
//		Long partitionKey = this.keyMaker.getPartitionKey(key, this.setName, this.nPartitions);
//		return this.getPartition(partitionKey);
//	}
	
	public DHT getPartition(Long partitionKey){
		if(this.partitions==null)
			this.makeStubs();
		return this.partitions.get(partitionKey);
	}
	
	public Long generatePartitionKey(int partitionN){
		return this.keyMaker.getPartitionKey(partitionN, setName, nPartitions);
	}
	
	public String getSetName() {
		return setName;
	}
	
	public Map<Long, DHT> getPartitions() {
		if(this.partitions==null)
			this.makeStubs();
		return partitions;
	}
	
	@Override
	public String toString() {
		String sysmapString = "";
		if(this.partitions==null)
			this.makeStubs();
		Set<Entry<Long,DHT>> partitions = this.partitions.entrySet();
		for (Entry<Long, DHT> entry : partitions) {
			DHT dht = entry.getValue();
			Long key = entry.getKey();
			sysmapString+="[["+dht.toString()+"]."+key+"] ";
		}
		return sysmapString;
	}
	
}
