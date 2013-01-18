package titan.sys.data;

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import vrs0.crdts.CRDTInteger;
import vrs0.crdts.ORMap;
import vrs0.crdts.runtimes.CRDTRuntime;



public class WordCountSet implements SysSet {
	
//	private ORMap<String, WordCount> wordCounts;
	private ORMap<String, CRDTInteger> wordCounts;

	public WordCountSet() {
//		this.wordCounts = new ORMap<String, WordCount>();
		this.wordCounts = new ORMap<String, CRDTInteger>();
	}
	
	private CRDTRuntime getRuntime(){
		CRDTRuntime runtime = new CRDTRuntime();
		runtime.setSiteId("WCSet");
//		this.set.setUpdatesRecorder(runtime);
		return runtime;
	}
	
	@Override
	public synchronized SysData getData() {
		Map<String, Set<CRDTInteger>> values = this.wordCounts.getValue();
//		Iterator<Set<CRDTInteger>> it = values.values().iterator();
		LinkedList<WordCount> wcs = new LinkedList<WordCount>();

		Set<Entry<String,Set<CRDTInteger>>> test = values.entrySet();
//		Iterator<Entry<String,Set<CRDTInteger>>> test2 = test.iterator();
		for (Entry<String, Set<CRDTInteger>> entry : test) {
			String word = entry.getKey();
			Set<CRDTInteger> counts = entry.getValue();
			for (CRDTInteger crdtInteger : counts) {
				WordCount wordCount = new WordCount(word,crdtInteger.value());
				wcs.add(wordCount);
			}
		}
		
//		Map<String, Set<WordCount>> values = this.wordCounts.getValue();
//		Iterator<Set<WordCount>> it = values.values().iterator();
//		LinkedList<WordCount> wcs = new LinkedList<WordCount>();
//		while(it.hasNext()){
//			Set<CRDTInteger> words = it.next();
//			Iterator<CRDTInteger> it2 = words.iterator();
//			Set<WordCount> words = it.next();
//			Iterator<WordCount> it2 = words.iterator();
//			while(it2.hasNext()){
//				wcs.add(it2.next());
//			}
//		}
		return new SysData(wcs);
	}
	
	@Override
	public Object add(Object data) {
		WordCount wordCount = (WordCount) data;
		String word = wordCount.getWord();
		synchronized (this.wordCounts) {
			if(this.wordCounts.lookup(word)){
				Set<CRDTInteger> counterSet = this.wordCounts.get(word);
				CRDTInteger counter = counterSet.iterator().next();
				counter.add(wordCount.getWordCount());
//				Integer value = this.wordCounts.getValue().get(word).iterator().next().getWordCount();
//				wordCount.add(value);
//				this.wordCounts.delete(word);
//				this.wordCounts.insert(word, wordCount);
			}else{
				CRDTInteger counter = new CRDTInteger(0);
				counter.add(wordCount.getWordCount());
				this.wordCounts.insert(word, counter);
//				this.wordCounts.insert(word, wordCount,this.getRuntime().nextEventClock());
			}
		}
		return null;
	}
	
	@Override
	public void merge(SysSet mergeableSet) {
//		WordCountSet set = (WordCountSet) mergeableSet;
//		this.set.merge(set.set);
	}
	
//	@Override
	public SysSet createEmpty() {
		WordCountSet set = new WordCountSet();
		return set;
	}
	
	@Override
	public String getPartitionName() {
		return "WordCountSet";
	}
	
}
