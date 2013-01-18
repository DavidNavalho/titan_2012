package titan.gateway.client.setup;

import titan.gateway.setup.SetFactory;
import titan.sys.data.SysSet;
import titan.sys.data.WordCountSet;

public class WCTweetSetFactory implements SetFactory {

	public WCTweetSetFactory() {
	}
	
	@Override
	public SysSet createEmpty() {
		return new WordCountSet();
	}
	
}
