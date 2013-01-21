package titan.sys.nodes;

import java.util.LinkedList;

import titan.sys.data.Sysmap;

public class SysmapHandler {
	
	//TODO: use a map instead of a list...
	//TODO: also, I don't really need those sysmapKeys.... - could even use a CRDT here...?
	private LinkedList<Sysmap> sysmaps;
	private LinkedList<String> sysmapKeys;
	
	public SysmapHandler() {
		this.sysmaps = new LinkedList<Sysmap>();
		this.sysmapKeys = new LinkedList<String>();
	}
	
	//TODO: dont really wanna do this like this, but I can change it later I guess...
	public void addSysmap(Sysmap sysmap, String key){
		if(!this.contains(key)){
			this.sysmaps.add(sysmap);
			this.sysmapKeys.add(key);
		}else{//TODO: replace current sysmap with new one...
			//TODO
			System.out.println("Duplicate Sysmap?!!!");
		}
	}
	
	//TODO: returning null also isnt helping much here...
	public Sysmap getSysmap(String key){
		int i=0;
		for (String sysmapKey : this.sysmapKeys) {
			if(sysmapKey.equalsIgnoreCase(key)){
				return this.sysmaps.get(i);
			}
			i++;
		}
		return null;
	}
	
	public boolean contains(String key){
		synchronized (sysmapKeys) {
			for (String sysmapKey : sysmapKeys) {
				if(sysmapKey.equalsIgnoreCase(key))
					return true;
			}}
		return false;
	}
}
