package titan.gateway.client.setup;

import titan.gateway.setup.SetFactory;
import titan.sys.data.SysSet;
import titan.sys.data.TopSet;

public class TopSetFactory implements SetFactory {

	public TopSetFactory() {
	}
	
	@Override
	public SysSet createEmpty() {
		return new TopSet();
	}
}
