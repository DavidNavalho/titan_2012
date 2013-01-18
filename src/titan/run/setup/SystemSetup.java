package titan.run.setup;

import sys.dht.api.DHT;
import titan.sys.SysNode;

public class SystemSetup {
	
	private DHT stub;

	public SystemSetup() {
//		sys.Sys.init();
//		stub = Sys.getDHT_ClientStub();
	}
	
	public void createNode(){
		SysNode sysnode = new SysNode();
		sysnode.initialize();
	}
	
	public static void main(String[] args) {
		sys.Sys.init();
		SystemSetup ss = new SystemSetup();
		ss.createNode();
	}
	
}
