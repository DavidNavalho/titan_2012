package utils;

public class Sleeper implements Runnable {
	
	protected long sleepTime = 100;//default 100ms

	public void setSleep(long sleepTime){
		this.sleepTime = sleepTime;
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(this.sleepTime);
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted (new data inserted)");
		}
	}

}
