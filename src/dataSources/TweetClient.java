package dataSources;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import tests.mixed_comm.DataExtractor;
import titan.sys.data.Tweet;
import utils.ClientData;
import utils.ClientsManager;


public class TweetClient extends UnicastRemoteObject implements ITweetClient, Runnable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String filePath = "";
	protected DataExtractor de = null;
	protected BlockingQueue<Tweet> tweets;
	
	protected TweetClient(String source) throws RemoteException, FileNotFoundException{
		super();
		this.filePath = source;
		de = new DataExtractor(this.filePath);
		this.tweets = new LinkedBlockingQueue<Tweet>();
	}
	
	@Override
	public void run() {
		
		Tweet tweet = null;
		try {
			while((tweet = de.readTweet()) != null)
				this.tweets.put(tweet);
			System.out.println("Ran out of tweets to read! Tweets left to serve: "+this.tweets.size());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//TODO: if it has no tweets, does it wait, or return immediately with null?
	//TODO: wait, for now...
	@Override
	public Tweet getTweet() throws RemoteException {
		try {
			return this.tweets.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public LinkedList<Tweet> getTweets() throws RemoteException {
		LinkedList<Tweet> tweets = new LinkedList<Tweet>();
		this.tweets.drainTo(tweets);
		return tweets;
	}
	
	@Override
	public LinkedList<Tweet> getTweets(int maxTweets) throws RemoteException {
		LinkedList<Tweet> tweets = new LinkedList<Tweet>();
		this.tweets.drainTo(tweets, maxTweets);
		return tweets;
	}
	
	@SuppressWarnings("unused")
	private void clientTests(){
		try {
			TweetClient tweetClient = new TweetClient("/Users/jinx/Documents/eclipse/Tinkering/test/checkin_data_1.txt");
			new Thread(tweetClient).start();
			Tweet firstTweet = tweetClient.getTweet();
			System.out.println(firstTweet);
			LinkedList<Tweet> tweets = tweetClient.getTweets();
			System.err.println("Printing "+tweets.size()+" tweets.");
			for (Tweet tweet : tweets) {
				System.out.println(tweet);
			}
			tweets = tweetClient.getTweets();
			System.err.println("Printing "+tweets.size()+" tweets.");
			for (Tweet tweet : tweets) {
				System.out.println(tweet);
			}
			tweets = tweetClient.getTweets();
			System.err.println("Printing "+tweets.size()+" tweets.");
			for (Tweet tweet : tweets) {
				System.out.println(tweet);
			}
			tweets = tweetClient.getTweets();
			System.err.println("Printing "+tweets.size()+" tweets.");
			for (Tweet tweet : tweets) {
				System.out.println(tweet);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.getProperties().put("java.security.policy","policy.all");
		if(System.getSecurityManager() == null)
			System.setSecurityManager(new RMISecurityManager());
		try{
			LocateRegistry.createRegistry(1099);
		}catch(RemoteException e){
			//do nothing
		}
//		String filePath = "/Users/jinx/Documents/eclipse/Tinkering/test/checkin_data_1.txt";
		String clientsPath = "ClientLocations.txt";
		int clientIndex = 1;
		if(args.length==2){
			clientsPath = args[0];
			clientIndex = new Integer(args[1]);
		}else if((args.length!=2) && (args.length!=0)){
			System.out.println("Incorrect usage. Please give location of configuration file and clientIndex:\r\n"+"TweetClient confFile #");
			System.exit(0);
		}
		try {
			ClientsManager cm = new ClientsManager(clientsPath);
			ArrayList<ClientData> sources = cm.getClients();
//			Iterator<ClientData> it = sources.iterator();
//			while(it.hasNext()){
//				ClientData cd = it.next();
				ClientData cd = sources.get(new Integer(clientIndex)-1);
				TweetClient client = new TweetClient(cd.getSourceFile());
				Naming.rebind("/"+cd.getServiceName(), client);
				System.err.println(cd.getServiceName()+" bound to registry.");
				new Thread(client).start();
//			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
