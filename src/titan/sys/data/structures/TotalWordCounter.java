package titan.sys.data.structures;

import java.util.LinkedList;

import titan.sys.data.SysSet;
import vrs0.crdts.CRDTInteger;
import vrs0.crdts.runtimes.CRDTRuntime;
import vrs0.exceptions.IncompatibleTypeException;

public class TotalWordCounter implements SysSet{

	private CRDTInteger counter;
	private CRDTRuntime runtime = null;
//	private Map<>
	
	protected boolean started = false;
	
	public TotalWordCounter() {
		this.counter = new CRDTInteger(0);
	}
	
	public TotalWordCounter(CRDTRuntime runtime){
		this.counter = new CRDTInteger(0);
		this.runtime = runtime;
	}
	
	public void setRuntime(CRDTRuntime runtime){
		this.runtime = runtime;
	}
	
	private void setInternalRuntime(){
		this.runtime = CRDTRuntime.getInstance();
		runtime.setSiteId("WordCount");
	}
	
	@Override
	public Object add(Object data) {
		if(this.runtime==null)
			this.setInternalRuntime();
		Integer wc = (Integer) data;
		int count = 0;
		synchronized (this.counter) {
			this.counter.add(wc,this.runtime.nextEventClock());
			count = this.counter.value();
		}
//		System.out.println("Total words: "+count);
//		if(!started){
//			logger();
//			started = true;
//		}
		return count;
	}
	
	public void logger(){
		Runnable run = new Runnable() {
			@Override
			public void run() {
				while(true){
					System.out.println("["+System.currentTimeMillis()+"]> Last count: "+counter.value());
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		new Thread(run).start();
	}
	
	@Override
	public void merge(SysSet mergeableSet) {
		if(this.runtime==null)
			this.setInternalRuntime();
		TotalWordCounter totalSet = (TotalWordCounter) mergeableSet;
		try {
			this.counter.merge(totalSet.counter, this.runtime.getCausalityClock(), totalSet.runtime.getCausalityClock());
		} catch (IncompatibleTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//	@Override
	public SysSet createEmpty() {
		
		return null;
	}

	@Override
	public SysData getData() {
		LinkedList<Integer> ll = new LinkedList<Integer>();
		ll.add(this.counter.value());
		return new SysData(ll);
	}
	
	@Override
	public String getPartitionName() {
		return "Total";
	}
	
}
