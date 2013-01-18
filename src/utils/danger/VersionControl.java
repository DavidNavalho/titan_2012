package utils.danger;

public class VersionControl {

	protected String id;
	protected int version;
	
	public VersionControl() {
		// TODO Auto-generated constructor stub
	}
	
	public VersionControl(String id){
		this.id = id;
		this.version = 0;
	}
	
	public void inc(){
		this.version++;
	}
	
	public String getId() {
		return id;
	}
	
	public int getVersion() {
		return version;
	}
}
