package utils;

public class ClientData {
	protected String host = "";
	protected String serviceName = "";
	protected String sourceFile = "";

	public ClientData() {
		// TODO Auto-generated constructor stub
	}
	
	public ClientData(String host, String serviceName, String sourceFile) {
		this.host = host;
		this.serviceName = serviceName;
		this.sourceFile = sourceFile;
	}
	
	public String getHost() {
		return host;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public String getSourceFile() {
		return sourceFile;
	}
}
