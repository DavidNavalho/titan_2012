package titan.sys.data;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import sys.dht.api.DHT;
import titan.gateway.setup.SetFactory;

public class PartitionKey implements DHT.Key{
	
	private static MessageDigest digest;
	protected String setName;
	protected Long key;
	protected SetFactory factory;
	
	public PartitionKey() {
		// TODO Auto-generated constructor stub
	}
	
	public PartitionKey(String setName, Long key, SetFactory setFactory){
		this.setName = setName;
		this.key = key;
		this.factory = setFactory;
	}
	
	public SetFactory getFactory() {
		return factory;
	}
	
	public String toString(){
		return setName+"_"+key+"";
	}
	
	//TODO: equals?
	
	@Override
	public long longHashValue() {
		synchronized(digest){
			digest.reset();
	        digest.update(key.byteValue());
	        return new BigInteger(1, digest.digest()).longValue() >>> 1;
		}
	}

	static {
        try {
            digest = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
	
}
