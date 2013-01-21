package titan.sys.handlers;

import sys.dht.api.DHT;
import titan.sys.messages.rpc.DataDelivery;

public interface DataManagerHandler {

	abstract class RequestHandler extends DHT.AbstractMessageHandler {
		abstract public void onReceive(DHT.Handle con, DHT.Key key, DataDelivery delivery);
	}
	
}
