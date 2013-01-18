package titan.sys.data;

import java.io.Serializable;
import java.util.Date;

public class Tweet implements Serializable{

	private static final long serialVersionUID = 1L;
	private String tweet;
	private double latitude;
	private double longitude;
	private String userID;
	private SysKey key;
	private Date date;
	private String tweetKey;
	
	public Tweet() {
		// TODO Auto-generated constructor stub
	}
	
	public Tweet(String tweet, double latitude, double longitude, String userID, Date date, String tweetKey) {
		this.tweet = tweet;
		this.latitude = latitude;
		this.longitude = longitude;
		this.userID = userID;
		this.date = date;
		this.tweetKey = tweetKey;
//		this.key = new SetKey(this.tweet+this.latitude+this.longitude+this.userID);
		this.key = new SysKey(this.tweetKey);
	}
	
	public String getKeyAsString(){
		return this.tweetKey;
	}
	
	public SysKey getTweetKey(){
		return this.key;
	}

	public String getTweet() {
		return tweet;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}
	
	@Override
	public String toString() {
		return (tweetKey+"|"+userID+": "+(this.tweet)+" | @["+this.latitude+", "+this.longitude+"]; "+this.date.toString());
	}
	
}
