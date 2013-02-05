package titan.sys.data;

import java.util.LinkedList;


public interface SysSet {
	public int add(Object data);
	public void merge(SysSet mergeableSet);
//	public SysSet createEmpty();
//	public String getSetKey();
	public SysData getData();
	public String getPartitionName();
	public void makeEmpty();
	
	public class SysData{
		
		private Object obj;
		private int dataSize;
		
		public SysData() {
		}
		
		public SysData(LinkedList<?> singleData){
			this.obj = singleData;
			this.dataSize = singleData.size();
		}
		
		public Object getObj() {
			return obj;
		}
		
		public int getDataSize() {
			return dataSize;
		}
	}
}
