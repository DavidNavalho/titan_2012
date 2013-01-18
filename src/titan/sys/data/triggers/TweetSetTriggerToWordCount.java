package titan.sys.data.triggers;

import java.util.HashMap;
import java.util.LinkedList;

import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import titan.sys.data.Sysmap;
import titan.sys.data.Tweet;
import titan.sys.data.WordCount;

public class TweetSetTriggerToWordCount extends Trigger{
	
//	private SysmapManager manager;
	private HashMap<String, WordCount> words;
//	private Sysmap targetSet;
	
	public TweetSetTriggerToWordCount() {
		// TODO Auto-generated constructor stub
	}

	public TweetSetTriggerToWordCount(Sysmap targetSet, int waitTime, int minimumLoad) {
		super(targetSet, "tweetSetTriggerToWordCount", waitTime, minimumLoad);
//		this.targetSet = targetSet;
//		this.manager = new SysmapManager(this.targetSet);
		this.words = new HashMap<String, WordCount>();
	}
	
	@Override
	public Object processData(LinkedList<Object> dataIn) {
//		System.err.println("Trigger processing...");
		for (Object object : dataIn) {
			Tweet tweet = (Tweet) object;
			this.countWords(tweet);
		}
		return null;
	}
	
	public void countWords(Tweet tweet){
		synchronized (this.words) {
			
		
		String[] words = tweet.getTweet().split(" ");
		for (String string : words) {
			if(this.words.containsKey(string)){
				WordCount wc = this.words.get(string);
				wc.add(1);
			}else{
				WordCount wc = new WordCount(string, 1);
				this.words.put(string, wc);
			}
		}}
	}
	
	@Override
	public synchronized void syncData(RpcEndpoint rpc, RpcHandler clientRpcHandler) {
		synchronized (words) {
			for (WordCount wc : words.values()) {
				this.manager.addData(wc, wc.getWCKey());
			}
//			System.out.println("Syncing data...");
//			this.manager.syncAndDiscard(stub);
//			this.manager.syncAndDiscard(rpc, clientRpcHandler);
			this.words.clear();
//			this.words = new HashMap<String, WordCount>();
		}
	}
	
}
