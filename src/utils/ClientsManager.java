package utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import dataSources.ITweetClient;



public class ClientsManager {
	
	protected ArrayList<ClientData> clients = new ArrayList<ClientData>();
	protected ArrayList<ITweetClient> clientsServices = new ArrayList<ITweetClient>();

	public ClientsManager() {
		// TODO Auto-generated constructor stub
	}
	
	public ClientsManager(String clientsFile) {
		try{
			FileInputStream fis = new FileInputStream(clientsFile);
			DataInputStream dis = new DataInputStream(fis);
			InputStreamReader isr = new InputStreamReader(dis);
			BufferedReader br = new BufferedReader(isr);
			String host,serviceName,sourceData;
			while((host = br.readLine()) != null){
				if(host.equalsIgnoreCase(""))
					break;
				serviceName = br.readLine();
				sourceData = br.readLine();
				this.clients.add(new ClientData(host,serviceName,sourceData));
			}
			br.close();
			System.out.println("All sources known.");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	private void findClients() {
		for (ClientData clientData : this.clients) {
			try {
				ITweetClient client = (ITweetClient) Naming.lookup("//"+clientData.host+"/"+clientData.serviceName);
				this.clientsServices.add(client);
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
	
	public ArrayList<ITweetClient> getClientsServices() {
		this.findClients();
		return this.clientsServices;
	}
	
	public ArrayList<ClientData> getClients() {
		return clients;
	}
	
}
