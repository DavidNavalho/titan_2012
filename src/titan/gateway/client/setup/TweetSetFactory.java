package titan.gateway.client.setup;

import titan.gateway.setup.SetFactory;
import titan.sys.data.SysSet;
import titan.sys.data.TweetSetv2;

public class TweetSetFactory implements SetFactory {

	public TweetSetFactory() {
	}
	
	@Override
	public SysSet createEmpty() {
		return new TweetSetv2();
	}

}
