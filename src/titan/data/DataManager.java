package titan.data;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sys.utils.Threading;
import titan.sys.SysHandler;
import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;
import titan.sys.data.SysmapManager;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;
import titan.sys.messages.rpc.RpcReply;

public class DataManager implements Runnable{
	
	//TODO: this should be in a .preferences file or something similar...

	private DataManagerHandler handler;
	private SysmapManager dataManager;
	private DataReader reader;
	//TODO: extend class to enable for loadCounts/timeCounts automatic syncing
	private long maxWaitTime;
//	protected int sleepTime;//this is used for when no new data is available/sync
	private int minimumLoad;
	private Integer loadCounter = 0;
	private int defaultSleep = 50;
	private Integer syncPool = new Integer(0);
	private int maxSyncPool = 4;
	private BlockingQueue<Data> data;
	public static Object lock = new Object();
	
	//There should be a DataManager per Sysmap Requested
	//e.g, if a Client requests a sysmap for Tweets insertion, it will turn into 1 DataManager (responsible with syncing with the various partitions)
	//e.g2,	A Trigger on the Twitter Resources (in this case, for ONE Partition), will request a sysmap to send words to. 
	// 		In effect, there will be 4 DataManagers created, one at each Twitter Resource Partition
	public DataManager() {
		// TODO Auto-generated constructor stub
	}
	
	public DataManager(Sysmap sysmap, long maxWaitTime, int minimumLoad) {
		this.maxWaitTime = maxWaitTime;
//		this.sleepTime = 50;//default sleep time: 50ms...
		this.minimumLoad = minimumLoad;
		this.data = new LinkedBlockingQueue<Data>();
		this.dataManager = new SysmapManager(sysmap);
//		this.maxSyncPool = 2*sysmap.partitions();
//		this.syncPool = this.maxSyncPool;
		this.reader = new DataReader(data, dataManager);
		this.handler = new DataManagerHandler();
//		this.endpoint = rpcFactory.toService(RpcServices.TITAN.ordinal(), this.handler);
//		if(this.endpoint==null)
//			System.err.println("Created null endpoint?!");
	}
	

	public void addData(Object data, SysKey key){
		try {
			this.data.put(new Data(data, key));
			Threading.synchronizedNotifyOn(DataManager.lock);
//			synchronized (syncObj) {
//				syncObj.notify();
//			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	//TODO: sync-only methods? discard-only(purge/garbage-collection?) methods? 
	public void syncAndDiscard(){
		this.dataManager.syncAndDiscard(this.handler);
//		System.out.println("Sent "+count+" data items!");
//		synchronized (this.loadCounter) {
//			this.loadCounter+=count;
//		}
//		Threading.synchronizedNotifyOn(DataManager.lock);
//		lock.lock();
//		try{
//			newResources.signal();
//		} finally{
//			lock.unlock();
//		}
//		synchronized (lockObj) {
//			lockObj.notify();
//		}
//		synchronized (this.syncPool) {
//			this.syncPool+=count;
//		}
	}
	
//	protected Sleeper sleeper = new Sleeper();
//	protected Thread sleeper = new Thread();
//	private final Object syncObj = new Object();
	
	@Override
	public void run() {
		long oldestTime = System.currentTimeMillis();
		long newestTime;
		boolean forceSync = false;
		
		while(true){
			if((this.loadCounter>this.minimumLoad) || ((forceSync) && this.loadCounter>1)){
				this.syncAndDiscard();
				synchronized (this.loadCounter) {
					this.loadCounter = 0;
				}
				forceSync = false;
			}else{
				newestTime = System.currentTimeMillis();
				long timeDiff = newestTime - oldestTime;
				if(maxWaitTime>timeDiff)
					Threading.synchronizedWaitOn(DataManager.lock, (maxWaitTime-timeDiff));
				else
					forceSync = true;
				oldestTime = System.currentTimeMillis();
			}
		}
	}
	
	private class DataManagerHandler extends SysHandler.ReplyHandler{
		
		public DataManagerHandler() {
			// TODO Auto-generated constructor stub
		}
		
		@Override
		public void onReceive(SetCreateReply reply) {
			// TODO Auto-generated method stub
		}
		@Override
		public void onReceive(SysmapCreateReply reply) {
			// TODO Auto-generated method stub
		}
		@Override
		public void onReceive(SysMessageReply reply) {
			// TODO Auto-generated method stub
		}
		@Override
		public void onReceive(RpcReply reply) {
			synchronized (syncPool) {
				syncPool++;
			}
		}
	}
	
//	private class ManagerRpcHandler extends TitanRpcHandler.RpcHandler{
//
//		@Override
//		public void onReceive(RpcHandle handle, DataDelivery m) {
//		}
//
//		@Override
//		public void onReceive(RpcHandle handle, RpcReply m) {
//			synchronized (syncPool) {
//				syncPool++;
//			}
//		}
//
//		@Override
//		public void onReceive(RpcHandle handle, TriggerDelivery m) {
//			// TODO Auto-generated method stub
//		}
//	}

	private class DataReader implements Runnable{
		private BlockingQueue<Data> queue;
		private SysmapManager dataManager;
		
		public DataReader() {
			// TODO Auto-generated constructor stub
		}
		
		public DataReader(BlockingQueue<Data> data, SysmapManager dataManager) {
			this.queue = data;
			this.dataManager = dataManager;
			new Thread(this).start();
		}
		//TODO: I am only sending one single resource at a time - I can probably improve this by checking if I can send multiple...
		@Override
		public void run() {
			while(true){
				try {
					Data data = this.queue.take();
					this.dataManager.addData(data.data, data.key);
					synchronized (loadCounter) {
						loadCounter++;
					}
//					Threading.synchronizedNotifyAllOn(DataManager.lock);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
