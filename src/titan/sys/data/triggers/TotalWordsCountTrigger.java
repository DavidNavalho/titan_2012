package titan.sys.data.triggers;

import java.util.LinkedList;

import titan.sys.data.SysKey;
import titan.sys.data.Sysmap;
import titan.sys.data.WordCount;

public class TotalWordsCountTrigger extends Trigger {
	
//	private Sysmap targetSet;
//	private SysmapManager manager;
	protected Integer wordCount;
	
	public TotalWordsCountTrigger() {
		// TODO Auto-generated constructor stub
	}
	
	public TotalWordsCountTrigger(Sysmap targetSet, int waitTime, int minimumLoad) {
		super(targetSet, "totalWordsCount", waitTime, minimumLoad);
//		this.targetSet = targetSet;
//		this.manager = new SysmapManager(this.targetSet);
		this.wordCount = new Integer(0);
	}
	
	@Override
	public Object processData(LinkedList<Object> dataIn) {
//		System.err.println("Trigger: Counting new words...");
		for(Object obj : dataIn){
			synchronized (wordCount) {
				WordCount newWords = (WordCount) obj;
				int newCount = newWords.getWordCount();
				this.wordCount+=newCount;
			}
		}
		return null;
	}

	@Override
	public void syncData() {
		// TODO Auto-generated method stub
		synchronized (this.wordCount) {//System.out.println("Sys> Sending wordCount to TotalWordsCountSet");
//			System.out.println("TTLSyncing data...");
			this.manager.addData(this.wordCount, new SysKey("TotalWordsCountSet"));
//			System.out.println("TTLdata synced...");
//			this.manager.syncAndDiscard(stub);
//			this.manager.syncAndDiscard(rpc, clientRpcHandler);
			this.wordCount = new Integer(0);
		}
	}

}
