package titan.sys.data;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import vrs0.crdts.ORMap;

public class TotalWordsCountSet implements SysSet {

	private ORMap<String,Integer> wordCounts;
	protected long loggingInterval;
	
	public TotalWordsCountSet() {
		this.wordCounts = new ORMap<String,Integer>();
		this.loggingInterval = 10000;
		this.logger();
	}
	
	@Override
	public Object add(Object data) {
		Integer count = (Integer) data;
		synchronized(this.wordCounts){
			if(!this.wordCounts.lookup("total")){
				this.wordCounts.insert("total", new Integer(0));
			}
			Integer previousTotal = this.wordCounts.get("total").iterator().next();
			count+=previousTotal;
			this.wordCounts.delete("total");
			this.wordCounts.insert("total", count);
		}
//		System.out.println("TotalWords> "+System.currentTimeMillis()+": Total words: "+count);
		return null;
	}

	@Override
	public void merge(SysSet mergeableSet) {
		// TODO Auto-generated method stub

	}

//	@Override
	public SysSet createEmpty() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized SysData getData() {
		LinkedList<Integer> total = new LinkedList<Integer>();
		total.add(this.wordCounts.get("total").iterator().next());
		return new SysData(total);
	}

	@Override
	public String getPartitionName() {
		return "TotalWordsCountSet";
	}
	
	public void logger(){
		Runnable run = new Runnable() {
			@Override
			public void run() {
//				RollingFileAppender rfa = null;
//				try {
//					rfa = new RollingFileAppender(new PatternLayout(), "/Users/jinx/Documents/eclipse/TitanProject/TotalCount.txt", true);
//				} catch (IOException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//				Logger logger;
//				logger = Logger.getLogger(TotalWordsCountSet.class.getName());
//				logger.setLevel(Level.INFO);
//				logger.addAppender(rfa);
				
//				logger.info(logLineTitle);
				
//				int mb = 1024*1024;
//				Runtime runtime = Runtime.getRuntime();
//				//Print used memory
//				logger.info("Used Memory:"
//		            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
//		        //Print free memory
//				logger.info("Free Memory:"
//		            + runtime.freeMemory() / mb);
//		        //Print total available memory
//				logger.info("Total Memory:" + runtime.totalMemory() / mb);
//		        //Print Maximum available memory
//				logger.info("Max Memory:" + runtime.maxMemory() / mb);
				
				while(true){
//					logger.info("["+System.currentTimeMillis()+"]> Last count: "+wordCounts.get("total"));
					System.out.println("["+System.currentTimeMillis()+"]> Last count: "+wordCounts.get("total"));
					try {
						Thread.sleep(loggingInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		new Thread(run).start();
	}
	
}
