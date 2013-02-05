package utils.concurrency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.dht.api.DHT;
import titan.data.Data;
import titan.sys.SysHandler;
import titan.sys.data.SysKey;
import titan.sys.data.SysSet;
import titan.sys.data.SysSet.SysData;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.rpc.DataDelivery;
import titan.sys.messages.rpc.RpcReply;
import utils.Sleeper;

public class PartitionManager{

	protected BlockingQueue<Data> data;
	protected SysSet sysset;
	protected long maxWaitTime;
	protected int minimumLoad;
	protected DHT stub;
	protected Long partitionKey;
	protected long lastSync;
	protected ManagerReplyHandler handler;
	protected DataReader reader;
	
	//I want this running on an Executor;
	//it needs to get data from outside (queue)
	//it needs to run a thread for reading this data
	//it needs a thread for timeouts
	
	public PartitionManager() {
		// TODO Auto-generated constructor stub
	}
	
	public PartitionManager(SysSet sysset, long maxWaitTime, int minimumLoad, DHT stub, Long partitionKey) {
		this.data = new LinkedBlockingQueue<Data>();
		this.sysset = sysset;
		this.maxWaitTime = maxWaitTime;
		this.minimumLoad = minimumLoad;
		this.stub = stub;
		this.partitionKey = partitionKey;
//		this.reader = new DataReader(this.data, this, this.minimumLoad);
		this.lastSync = System.currentTimeMillis();
//		this.handler = new ManagerReplyHandler();
//		new Thread(new TimeOut(this.maxWaitTime, this)).start();
	}
	
	public void addData(Object data, SysKey key){
		try{
			this.data.put(new Data(data, key));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	protected int addData(Object data){
		synchronized (this.sysset) {
			int count = 1;
			count = this.sysset.add(data);
			return count;
		}
	}
	
	protected void syncAndDiscard(){
		synchronized (this.sysset) {
			SysData data = this.sysset.getData();
			this.stub.send(new SysKey(this.partitionKey), new DataDelivery("addData",data,this.partitionKey));//TODO: no timeout?
			this.sysset.makeEmpty();
			this.lastSync = System.currentTimeMillis();
		}
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
	
	private class TimeOut implements Runnable{
		
		protected long timeout;
		protected PartitionManager partitionManager;
		
		@SuppressWarnings("unused")
		public TimeOut() {
			// TODO Auto-generated constructor stub
		}
		
		public TimeOut(long timeOut, PartitionManager partitionManager) {
			this.timeout = timeOut;
			this.partitionManager = partitionManager;
		}
		
		@Override
		public void run() {
			while(true){
				Sleeper sleeper = new Sleeper();
				long currentTime = System.currentTimeMillis();
				if((currentTime-this.partitionManager.lastSync)>=this.timeout)
					this.partitionManager.syncAndDiscard();
				else
					sleeper.sleep(this.timeout-Math.abs(currentTime-this.partitionManager.lastSync));
			}
		}
	}
	
	private class ManagerReplyHandler extends SysHandler.ReplyHandler{
		@Override
		public void onReceive(RpcReply reply) {
			// TODO Auto-generated method stub
			System.out.println(">>>Reply!<<<");
		}
		@Override
		public void onReceive(SetCreateReply reply) {
			// TODO Auto-generated method stub
			System.out.println("bleh");
		}
		@Override
		public void onReceive(SysmapCreateReply reply) {
			// TODO Auto-generated method stub
			System.out.println("blih");
		}
		@Override
		public void onReceive(SysMessageReply reply) {
			// TODO Auto-generated method stub
			System.out.println("bloh");
		}
	}
	
}
