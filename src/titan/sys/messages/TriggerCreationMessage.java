package titan.sys.messages;

import sys.dht.api.DHT;
import titan.sys.SysHandler;
import titan.sys.data.triggers.Trigger;
import utils.danger.VersionControl;

public class TriggerCreationMessage implements DHT.Message{
	
	private Trigger trigger;
	private String setName;
	
	public TriggerCreationMessage() {
		// TODO Auto-generated constructor stub
	}
	
	public TriggerCreationMessage(Trigger trigger, String setName){
		this.trigger = trigger;
		this.setName = setName;
	}
	
	protected VersionControl vc;
	public void setVC(VersionControl vc){
		this.vc = vc;
	}
	public VersionControl getVc() {
		return vc;
	}
	
	public String getSetName() {
		return setName;
	}
	
	public Trigger getTrigger() {
		return trigger;
	}
	
	@Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((SysHandler.RequestHandler) handler).onReceive(conn, key, this);
    }
	
}
