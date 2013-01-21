package titan.sys.nodes;

import java.util.HashMap;
import java.util.Map;

import titan.sys.data.SysSet;
import titan.sys.data.triggers.Trigger;

public class PartitionNodeHandler {

	private Map<Long, PartitionNode> partitionNodes;
	
	public PartitionNodeHandler() {
		this.partitionNodes = new HashMap<Long, PartitionNode>();
	}
	
	public void addNode(PartitionNode node){
		Long nodeKey = node.getKey();
		if(!this.partitionNodes.containsKey(nodeKey)){
			this.partitionNodes.put(node.getKey(), node);
			System.err.println("Sys> PartitionKey: "+node.getKey());
		}else{
			System.err.println("Sys> Partition "+node.getKey()+" already exists!");
		}
	}
	
	public void addMessage(Long partitionKey, Object message){
		PartitionNode node = this.partitionNodes.get(partitionKey);
		node.addData(message);
	}
	
	public int addData(Long partitionKey, Object data){
		PartitionNode node = this.partitionNodes.get(partitionKey);
		return node.addBulkData(data);//mergeSet((SysSet)data);
	}
	
	public void addTrigger(Long partitionKey, Trigger trigger){
		PartitionNode node = this.partitionNodes.get(partitionKey);
		System.out.println("Adding trigger "+trigger.getTriggerName()+" to partition: "+partitionKey);
		System.out.println("PartitionNode: "+node.toString());
		node.addTrigger(trigger);
		System.out.println("Sys> Trigger "+trigger.getTriggerName()+" added to partition "+node.getSet().getPartitionName()+"_"+partitionKey);
	}

	public void mergeData(Long partitionKey, SysSet data){
		PartitionNode node = this.partitionNodes.get(partitionKey);
		node.mergeSet(data);
	}
	
	public boolean exists(Long partitionKey){
		if(this.partitionNodes.containsKey(partitionKey))
			return true;
		return false;
	}
}
