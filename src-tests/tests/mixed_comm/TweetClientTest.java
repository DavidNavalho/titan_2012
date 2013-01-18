package tests.mixed_comm;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.LinkedList;

import titan.sys.data.Tweet;
import dataSources.ITweetClient;

public class TweetClientTest {

	public static void main(String[] args) {
		String server = "localhost";
		String name = "twitterClient1";
		
		try {
			ITweetClient client = (ITweetClient) Naming.lookup("//"+server+"/"+name);
			Tweet tweet = client.getTweet();
			System.out.println(tweet);
			LinkedList<Tweet> tweets = client.getTweets(20);
			for (Tweet tweet2 : tweets) {
				System.out.println(tweet2);
			}
			LinkedList<Tweet> moreTweets = client.getTweets();
			System.err.println("Got "+moreTweets.size()+" more tweets!");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
