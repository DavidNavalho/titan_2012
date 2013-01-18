package titan.sys.data.triggers;

import java.util.LinkedList;

import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import titan.data.DataManager;
import titan.sys.data.Sysmap;

public abstract class Trigger {

	protected Sysmap targetSet;
	protected String triggerName = "iPlaceholder";
	protected DataManager manager;
	protected int waitTime = 0;
	protected int minimumLoad = 1;
	
	public Trigger() {
		// TODO Auto-generated constructor stub
	}
	
	public Trigger(Sysmap targetSet, String triggerName, int waitTime, int minimumLoad) {
		this.targetSet = targetSet;
		this.triggerName = triggerName;
		this.waitTime = waitTime;
		this.minimumLoad = minimumLoad;
		this.manager = null;
	}
	
	public void setManager(){
		this.manager = new DataManager(this.targetSet, this.waitTime, this.minimumLoad);
		new Thread(this.manager).start();
	}
	
	public String getTriggerName() {
		return triggerName;
	}
	
	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}
	
	//this needs to be overriden...
	public abstract Object processData(LinkedList<Object> dataIn);

	//this needs to be overriden...
	public abstract void syncData(RpcEndpoint rpc, RpcHandler clientRpcHandler);
	
	//I should change this - I should only need to have a setName as target, right?
	public Sysmap getTargetSet(){
		return this.targetSet;
	}
}
