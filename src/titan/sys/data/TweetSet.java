package titan.sys.data;

import java.util.Iterator;
import java.util.LinkedList;

import sys.dht.catadupa.crdts.CRDTRuntime;
import sys.dht.catadupa.crdts.ORSet;

public class TweetSet implements SysSet {
	
	private ORSet<Tweet> set;
	private CRDTRuntime runtime = null;
	
	public TweetSet() {
		this.set = new ORSet<Tweet>();
		this.runtime = new CRDTRuntime("TweetSet");//TODO: is this correct? - i dont think it is....shouldnt it be based on the partition?
		this.set.setUpdatesRecorder(this.runtime);
	}
	
	@Override
	public void makeEmpty() {
		this.set = new ORSet<Tweet>();
		this.set.setUpdatesRecorder(this.runtime);
	}
	
//	private void prepare(){
//		this.runtime = new CRDTRuntime("TweetSet");//TODO: is this correct? - i dont think it is....shouldnt it be based on the partition?
//		this.set.setUpdatesRecorder(this.runtime);
//		this.setup = true;
//	}
	
	private CRDTRuntime getRuntime(){
//		CRDTRuntime runtime = new CRDTRuntime("TweetSet");
//		this.set.setUpdatesRecorder(runtime);
		return this.runtime;
	}
	
	@Override
	public SysData getData() {
		Iterator<Tweet> it = set.iterator();
		LinkedList<Tweet> tweets = new LinkedList<Tweet>();
		while(it.hasNext()){
			tweets.add(it.next());
		}
		return new SysData(tweets);
	}

	//TODO
	@Override
	public int add(Object data) {
		CRDTRuntime runtime = this.getRuntime();
		Tweet tweet = (Tweet) data;
		synchronized (this.set) {
			this.set.add(tweet, runtime.getCausalityClock().recordNext("TweetSet"));
		}
		return 1;
	}

	@Override
	public void merge(SysSet mergeableSet) {
//		CRDTRuntime runtime = this.getRuntime();
		TweetSet tweetSet = (TweetSet) mergeableSet;
		this.set.merge(tweetSet.set);
	}

//	@Override
	public SysSet createEmpty() {
//		CRDTRuntime runtime = this.getRuntime();
		TweetSet tweet = new TweetSet();
		return tweet;
	}

//	@Override
//	public String getSetKey() {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public String getPartitionName() {
		return "TweetSet";
	}
	
}
