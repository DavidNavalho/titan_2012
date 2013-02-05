package utils;

public class Sleeper {
	
	public Sleeper() {
	}
	
	public void sleep(long sleeptime){
		try {
			Thread.sleep(sleeptime);
		} catch (InterruptedException e) {
//			System.out.println("Thread interrupted (new data inserted)");
			e.printStackTrace();
		}
	}
	
}
