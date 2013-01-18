package titan.gateway.client.setup;

import titan.gateway.setup.SetFactory;
import titan.sys.data.SysSet;
import titan.sys.data.structures.TotalWordCounter;

public class TotalSetFactory implements SetFactory {
	
	public TotalSetFactory(){
		
	}

	@Override
	public SysSet createEmpty() {
		return new TotalWordCounter();
	}

}
