package titan.sys.data;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import sys.dht.api.DHT;

public class SysKey implements DHT.Key, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static MessageDigest digest;
	private String key;
	protected Long realKey = null;

	public SysKey() {
		// TODO Auto-generated constructor stub
	}
	
	public SysKey(String key) {
		this.key = key;
	}
	
	public SysKey(Long realKey){
		this.realKey = realKey;
	}
	
	public String toString() {
		return key;
	}
	
	/*public boolean isBiggerThan(SysKey otherKey){
		if(this.key.compareTo(otherKey.key)>0)
			return true;
		return false;
	}*/
	
	//TODO: don't think this is the right way....but it suffices for now
	public boolean compareTo(SysKey key){
		if(this.key.toString().equalsIgnoreCase(key.toString()))
			return true;
		return false;
	}
	
	@Override
	public boolean equals(Object arg0) {
		return this.compareTo((SysKey) arg0);
	}
	
	
	
//	/**
//	 * 
//	 * @return This should be overwritten to accomodate the several partitioning mechanisms
//	 */
//	public long partitionKey(){
//		return this.longHashValue();
//	}
	
	//TODO: I suppose key extensions should override this? or should it be a sepparate mechanism? - sepparate for now
	@Override
	public synchronized long longHashValue() {
		if(this.realKey!=null){
			return this.realKey;
		}
		synchronized(digest){
			digest.reset();
	        digest.update(key.getBytes());
	        this.realKey = new BigInteger(1, digest.digest()).longValue() >>> 1;
	        return this.realKey;
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
