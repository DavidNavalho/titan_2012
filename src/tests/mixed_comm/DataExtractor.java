package tests.mixed_comm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Scanner;

import titan.sys.data.Tweet;

public class DataExtractor {
	
	private File file;
	private Scanner scanner;
	
	//TODO: do not use scanner....
	public DataExtractor(String filePath) throws FileNotFoundException {
		this.file = new File(filePath);
		this.scanner = new Scanner(new FileReader(this.file));
	}
	/**
	 * Example tweet (UserID\tTweetID\tLatitude\tLongitude\tCreatedAt\tText\tPlaceID):
	 * 41418752	19395304894	38.929854	-77.027976	2010-07-24 04:25:41	I'm at The Wonderland Ballroom (1101 Kenyon St NW, at 11th St, Washington, DC) w/ 2 others. http://4sq.com/2z5p82	8dc80c56f429dd1e
	 */
	public Tweet readTweet() throws ParseException{
		if(this.scanner.hasNextLine()){
			while(true){
				Tweet tweet = this.processLine(this.scanner.nextLine());
				if(tweet!=null)
					return tweet;
			}
		}
		else
			return null;
	}
	
	private Tweet processLine(String checkin) throws ParseException{
		try{
			Scanner scanner = new Scanner(checkin);
			scanner.useDelimiter("\t");
			String userID = scanner.next();
			String tweetID = scanner.next();
			double latitude = new Double(scanner.next());
			double longitude = new Double(scanner.next());
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = (Date)(formatter.parse(scanner.next()));
			String text = scanner.next();
	//		String placeID = scanner.next();
			return new Tweet(text, latitude, longitude, userID, date, tweetID);
		}catch(NoSuchElementException e){
//			System.err.println("Error reading: "+checkin);
			return null;
		}
	}
	
	public int countWords(Tweet tweet){
//		System.out.println("Tweet: "+tweet.getTweet());
		String[] words = tweet.getTweet().split(" ");
//		System.out.println("\tSize:"+words.length);
		return words.length;
	}
	

	public static void main(String[] args) {
		try {
			DataExtractor de = new DataExtractor("/Users/jinx/Documents/eclipse/Tinkering/test/checkin_data_1.txt");
			Tweet t = de.readTweet();
			int total = 0;
			int progress = 0;
			int count;
			while(t!=null){
				count = de.countWords(t);
				total+=count;
				progress+=count;
				t = de.readTweet();
				if(progress>=100000){
					System.out.println("Lastest count: "+total);
					progress=0;
				}
			}
			System.out.println("Counting finished:"+total);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
