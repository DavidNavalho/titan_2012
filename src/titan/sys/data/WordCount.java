package titan.sys.data;

import java.io.Serializable;

public class WordCount implements Serializable {

	private String word;
	private int wordCount;
	
	public WordCount() {
		// TODO Auto-generated constructor stub
	}
	
	public WordCount(String word, int count) {
		this.word = word;
		this.wordCount = count;
	}
	
	public int add(int i){
		this.wordCount+=i;
		return this.wordCount;
	}
	
	public SysKey getWCKey(){
		return new SysKey(this.word);
	}
	
	public String getWord() {
		return word;
	}
	
	public int getWordCount() {
		return wordCount;
	}
}
