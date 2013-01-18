package titan.sys;

import sys.dht.api.DHT;
import titan.sys.messages.SetCreation;
import titan.sys.messages.SysMessage;
import titan.sys.messages.SysmapCreationMessage;
import titan.sys.messages.TriggerCreationMessage;
import titan.sys.messages.replies.SetCreateReply;
import titan.sys.messages.replies.SysMessageReply;
import titan.sys.messages.replies.SysmapCreateReply;

public interface SysHandler {

	 /**
     * Denotes collection of requests/messages that the SysNode server processes/expects
     * 
     * @author smd, jinx
     *
     */
    abstract class RequestHandler extends DHT.AbstractMessageHandler {
        abstract public void onReceive(DHT.Handle con, DHT.Key key, SysMessage request);
        abstract public void onReceive(DHT.Handle con, DHT.Key key, SetCreation message);
        abstract public void onReceive(DHT.Handle con, DHT.Key key, TriggerCreationMessage message);
        abstract public void onReceive(DHT.Handle con, DHT.Key key, SysmapCreationMessage message);
    }

    /**
     * Denotes collection of reply/messages that the KVS client processes
     * 
     * @author smd
     *
     */
    abstract class ReplyHandler extends DHT.AbstractReplyHandler {
        abstract public void onReceive(SysMessageReply reply);
        abstract public void onReceive(SysmapCreateReply reply);
        abstract public void onReceive(SetCreateReply reply);
    }
    
}
