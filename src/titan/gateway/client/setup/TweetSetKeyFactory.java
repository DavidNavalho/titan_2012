package titan.gateway.client.setup;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import titan.gateway.setup.PartitionKeyFactory;
import titan.sys.data.SysKey;

public class TweetSetKeyFactory implements PartitionKeyFactory {
	
	private static MessageDigest digest;

	
	public TweetSetKeyFactory() {
		
	}

	//TODO: use tweetkey instead of syskey....
	public Long getPartitionKey(SysKey dataKey, String setName, int nPartitions) {
		synchronized(digest){
			digest.reset();
			Long tweetKey = dataKey.longHashValue();
			int partition = (Math.abs(tweetKey.intValue()%4))+1;
			digest.update((setName+partition).getBytes());
			return new BigInteger(1, digest.digest()).longValue() >>> 1;
		}
	}

	@Override
	public Long getPartitionKey(int partition, String setName, int totalPartitions) {
		synchronized(digest){
			digest.reset();
	        digest.update((setName+partition).getBytes());
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
