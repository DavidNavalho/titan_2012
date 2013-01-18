package titan.sys.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import swift.crdt.IntegerTxnLocal;
import sys.dht.catadupa.crdts.CRDTRuntime;
import sys.dht.catadupa.crdts.ORSet;

public class TopSet implements SysSet {
	
	private ORSet<WordCount> set;
	private HashMap<String, IntegerTxnLocal> wordCount;
//	private TxnHandle txn;

	public TopSet() {
		this.set = new ORSet<WordCount>();
		this.wordCount = new HashMap<String, IntegerTxnLocal>();
//		this.txn = new TxnTester("top", ClockFactory.newClock());
		this.sortedList = new LinkedList<WordCount>();
		this.existingWords = new HashMap<String, String>();
	}
	
	private CRDTRuntime getRuntime(){
		CRDTRuntime runtime = new CRDTRuntime("TOPSet");
		this.set.setUpdatesRecorder(runtime);
		return runtime;
	}
	
	@Override
	public SysData getData() {//TODO: wrong, not using...
		Iterator<WordCount> it = set.iterator();
		LinkedList<WordCount> wcs = new LinkedList<WordCount>();
		while(it.hasNext()){
			wcs.add(it.next());
		}
		return new SysData(wcs);
	}
	
	@Override
	public synchronized Object add(Object data) {
		/*
		  try{
			WordCount wc = (WordCount) data;
			String word = wc.getWord();
			if(this.wordCount.containsKey(word)){
				TxnHandle txn = new TxnTester("top", ClockFactory.newClock());
				IntegerTxnLocal txnLocal = txn.get(new CRDTIdentifier(word,"Int"), true, IntegerVersioned.class);
				txnLocal.add(wc.getWordCount());
				this.wordCount.put(word, txnLocal);
			}else{
				IntegerTxnLocal txnLocal = this.wordCount.get(word);
				txnLocal.add(wc.getWordCount());
			}
		}catch(Exception e){e.printStackTrace();}
		
		*/
		this.addWord((WordCount)data);
		this.printTOP();

//		/*
//		CRDTRuntime runtime = this.getRuntime();
//		WordCount wc = (WordCount) data;
//		System.out.println("WORDCOUNT: "+this.set.toString());//wc.getWord()+" -> "+wc.getWordCount());
//		this.set.add(wc, runtime.getCausalityClock().recordNext("TOPSet"));
		
//		*/
		return null;
	}
	//################PLACEHOLDER
	//################PLACEHOLDER	//################PLACEHOLDER
	//################PLACEHOLDER	//################PLACEHOLDER
	//################PLACEHOLDER
	private LinkedList<WordCount> sortedList;
	private HashMap<String,String> existingWords;
	private void printTOP(){
		synchronized (this.sortedList) {
			
		
		System.out.println("TOP WORDS: ");
		int max = 50;
		if(this.sortedList.size()<max)
			max = this.sortedList.size();
		for(int i=0;i<max;i++){
			WordCount wc = this.sortedList.get(i);
			if(wc!=null)
				System.out.println(i+1+": ["+wc.getWordCount()+"] "+wc.getWord());
		}
		}
	}
	private void addWord(WordCount word){
		synchronized (existingWords) {
			synchronized (sortedList) {
				String newWord = word.getWord();
				if(this.existingWords.containsKey(newWord)){
					//remove word from the list, 
					this.removeWord(word.getWord());
					//...add the new one ordered
					this.addWordOrdered(word);
				}else{
					this.existingWords.put(newWord, newWord);
					this.addWordOrdered(word);
				}
			}
		}
	}
	private void removeWord(String word){
		int i=0;
		for (WordCount wc : this.sortedList) {
			if(wc.getWord().equals(word)){
				this.sortedList.remove(i);
				return;
			}
			i++;
		}
	}
	
	private void addWordOrdered(WordCount wc){
		int count = wc.getWordCount();
		int i=0;
		for (WordCount current : this.sortedList) {
			int currentWordCount = current.getWordCount();
			if(count>=currentWordCount){// >= so it adds and stops right away...
				this.sortedList.add(i,wc);
				return;
			}
			i++;
		}//else it's the lowest...
		this.sortedList.add(wc);
	}
	
	//################PLACEHOLDER
	//################PLACEHOLDER
	//################PLACEHOLDER
	//################PLACEHOLDER	//################PLACEHOLDER
	//################PLACEHOLDER
	
	@Override
	public void merge(SysSet mergeableSet) {
		TopSet set = (TopSet) mergeableSet;
		this.set.merge(set.set);
	}
	
//	@Override
	public SysSet createEmpty() {
		TopSet set = new TopSet();
		return set;
	}
	
	@Override
	public String getPartitionName() {
		return "TopSet";
	}

}
