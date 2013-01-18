package tests.mixed_comm;

import java.util.HashMap;
import java.util.LinkedList;

import titan.sys.data.Tweet;
import titan.sys.data.TweetSet;
import titan.sys.data.TweetSetv2;
import titan.sys.data.WordCount;
import titan.sys.data.WordCountSet;
import titan.sys.data.structures.TotalWordCounter;

public class CRDTTests {
	
	static void testTotalWords(DataExtractor reader, int tweetsToRead) throws Exception{
		TotalWordCounter set = new TotalWordCounter();
		int inserted = 0;
		System.out.println("TotalWords,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				String[] words = tweet.getTweet().split(" ");
				Integer count = new Integer(words.length);
				set.add(count);
//				inserted+=words.length;
			}
			long endTime = System.currentTimeMillis();
			System.out.println(""+((LinkedList<Integer>)set.getData().getObj()).getFirst()+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}
	
	static void testWordCount(DataExtractor reader, int tweetsToRead) throws Exception{
		WordCountSet set = new WordCountSet();
		int inserted = 0;
		System.out.println("WordCount,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				String[] words = tweet.getTweet().split(" ");
				for (String string : words){
					WordCount wc = new WordCount(string, 1);
					set.add(wc);
				}
				inserted+=words.length;
			}
			long endTime = System.currentTimeMillis();
			System.out.println(""+inserted+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}
	
	static void testMultipleWordCount(DataExtractor reader, int tweetsToRead) throws Exception{
		WordCountSet[] sets = new WordCountSet[4];
		WordCountSet set1 = new WordCountSet();
		WordCountSet set2 = new WordCountSet();
		WordCountSet set3 = new WordCountSet();
		WordCountSet set4 = new WordCountSet();
		sets[0] = set1;
		sets[1] = set2;
		sets[2] = set3;
		sets[3] = set4;
		int currentSet = 1;
		int inserted = 0;
		System.out.println("WordCount,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				String[] words = tweet.getTweet().split(" ");
				for (String string : words){
					WordCount wc = new WordCount(string, 1);
					sets[currentSet-1].add(wc);
				}
				inserted+=words.length;
			}
			long endTime = System.currentTimeMillis();
			if((inserted/currentSet)>=1000000){
				currentSet++;
				System.out.println("Switched sets!");
			}
			System.out.println(""+inserted+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}
	
	static void testTweets(DataExtractor reader, int tweetsToRead) throws Exception{
		TweetSet set = new TweetSet();
		int inserted = 0;
		System.out.println("TotalTweets,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				set.add(tweet);
			}
			long endTime = System.currentTimeMillis();
			inserted+=tweetsToRead;
			System.out.println(""+inserted+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}
	
	static void testTweetsV2(DataExtractor reader, int tweetsToRead) throws Exception{
		TweetSetv2 set = new TweetSetv2();
		int inserted = 0;
		System.out.println("TotalTweets,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				set.add(tweet);
			}
			long endTime = System.currentTimeMillis();
			inserted+=tweetsToRead;
			System.out.println(""+inserted+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}

	static void testORMap(DataExtractor reader, int tweetsToRead){
	}
	
	static void testHashMap(DataExtractor reader, int tweetsToRead) throws Exception{
		HashMap<String, Tweet> map = new HashMap<String, Tweet>();
		int inserted = 0;
		System.out.println("TotalHashedTweets,TotalTime,TimePerOp");
		System.out.println("0,0,0");
		long initTime = System.currentTimeMillis();
		while(true){
			long startTime = System.currentTimeMillis();
			for(int i=0;i<tweetsToRead;i++){
				Tweet tweet = reader.readTweet();
				if(tweet==null) return;
				map.put(tweet.getKeyAsString(),tweet);
			}
			long endTime = System.currentTimeMillis();
			inserted+=tweetsToRead;
			System.out.println(""+inserted+","+(endTime-initTime)+","+(endTime-startTime));
		}
	}
	
	public static void main(String[] args) throws Exception{
		//test TweetSet
		int tweetsToRead = 1000;
		String dataLocation = "/Users/jinx/Documents/eclipse/Tinkering/test/checkin_data_1.txt";
		DataExtractor reader = new DataExtractor(dataLocation);
		
		//Test TweetSet
//		CRDTTests.testTweets(reader, tweetsToRead);
		//Test TweetSetv2
//		CRDTTests.testTweetsV2(reader, tweetsToRead);
		
		//Test WordCountSet -> Parece limitado pela mem—ria (permitida ao java) -> cresce MUITO (demais?)
//		CRDTTests.testWordCount(reader, tweetsToRead);
		//Test Multiple WordCountSets
//		CRDTTests.testMultipleWordCount(reader, tweetsToRead);
		
		//Test TotalWordsSet
//		CRDTTests.testTotalWords(reader, tweetsToRead);
		
		//Test java HashMap
		CRDTTests.testHashMap(reader, tweetsToRead);
	}
}
