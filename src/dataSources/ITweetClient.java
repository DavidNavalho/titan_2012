package dataSources;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.LinkedList;

import titan.sys.data.Tweet;

public interface ITweetClient extends Remote{

	public Tweet getTweet() throws RemoteException;
	public LinkedList<Tweet> getTweets() throws RemoteException;
	public LinkedList<Tweet> getTweets(int maxTweets) throws RemoteException;
}
