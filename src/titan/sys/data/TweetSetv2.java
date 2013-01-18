package titan.sys.data;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import vrs0.crdts.ORMap;
import vrs0.crdts.runtimes.CRDTRuntime;

public class TweetSetv2 implements SysSet {
	
	private ORMap<String,Tweet> set;
//	private CRDTRuntime runtime = null;
	
	public TweetSetv2() {
		this.set = new ORMap<String,Tweet>();
//		this.runtime = new CRDTRuntime("TweetSet");//TODO: is this correct? - i dont think it is....shouldnt it be based on the partition?
//		this.set.setUpdatesRecorder(this.runtime);
	}
	
//	private void prepare(){
//		this.runtime = new CRDTRuntime("TweetSet");//TODO: is this correct? - i dont think it is....shouldnt it be based on the partition?
//		this.set.setUpdatesRecorder(this.runtime);
//		this.setup = true;
//	}
	
	private CRDTRuntime getRuntime(){
		CRDTRuntime runtime = new CRDTRuntime();
		runtime.setSiteId("TweetSet");
//		this.set.setUpdatesRecorder(runtime);
		return runtime;
	}
	
	@Override
	public SysData getData() {
		LinkedList<Tweet> tweets = new LinkedList<Tweet>();
		Set<Entry<String,Set<Tweet>>> iterable = this.set.getValue().entrySet();
		for (Entry<String, Set<Tweet>> entry : iterable) {
			Set<Tweet> tweetSet = entry.getValue();
			if(tweetSet.size()>1)
				System.err.println("More than one tweet with the same key!");
			tweets.add(tweetSet.iterator().next());
		}
		return new SysData(tweets);
	}

	//TODO
	@Override
	public Object add(Object data) {
//		CRDTRuntime runtime = this.getRuntime();
		Tweet tweet = (Tweet) data;
		synchronized (this.set) {//TODO:Ill assume its always new for now...
//			this.set.add(tweet, runtime.getCausalityClock().recordNext("TweetSet"));
			this.set.insert(tweet.getKeyAsString(), tweet);
		}
		return null;
	}

	@Override
	public void merge(SysSet mergeableSet) {
//		CRDTRuntime runtime = this.getRuntime();
//		TweetSetv2 tweetSet = (TweetSetv2) mergeableSet;
//		this.set.merge(tweetSet.set);
	}

//	@Override
	public SysSet createEmpty() {
//		CRDTRuntime runtime = this.getRuntime();
		TweetSetv2 tweet = new TweetSetv2();
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
