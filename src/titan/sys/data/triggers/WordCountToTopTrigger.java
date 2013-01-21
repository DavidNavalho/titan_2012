package titan.sys.data.triggers;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import titan.sys.data.Sysmap;
import titan.sys.data.WordCount;

public class WordCountToTopTrigger extends Trigger{
	
//	private Sysmap targetSet;
//	private SysmapManager manager;
	private LinkedList<WordCount> sortedList;
	private HashMap<String,String> existingWords;
	private int top;

	public WordCountToTopTrigger() {
		// TODO Auto-generated constructor stub
	}
	
	public WordCountToTopTrigger(Sysmap targetSet, int top, int waitTime, int minimumLoad){
		super(targetSet, "wordCountToTop", waitTime, minimumLoad);
//		this.targetSet = targetSet;
//		this.manager = new SysmapManager(this.targetSet);
		this.sortedList = new LinkedList<WordCount>();
		this.existingWords = new HashMap<String, String>();
		this.top = top;
	}
	
	@Override
	public Object processData(LinkedList<Object> dataIn) {
		System.err.println("Trigger: Ordering new data...");
		for (Object object : dataIn) {
			WordCount wc = (WordCount) object;
			this.addWord(wc);
		}
		return null;
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
	
	@Override
	public void syncData() {
		synchronized (this.sortedList) {
			int i=0;
			Iterator<WordCount> it = this.sortedList.iterator();
			while(i<this.top){
				WordCount wc = it.next();
				this.manager.addData(wc, wc.getWCKey());
				i++;
			}
//			this.manager.syncAndDiscard(stub);
//			this.manager.syncAndDiscard(rpc, clientRpcHandler);
			//I don't clean this...should always stay updated I guess...
		}
		// TODO Auto-generated method stub
		
	}
}
