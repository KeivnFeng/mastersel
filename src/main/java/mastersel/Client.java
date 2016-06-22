package mastersel;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
	private static Logger log = LoggerFactory.getLogger(Client.class);

	private static final int ZOOKEEPER_CLIENTS = 10;
	private static final String ZOOKEEPER_SERVER_ADDRESS = "192.168.10.20:2181,192.168.10.20:2182,192.168.10.20:2183";
	
	public static void main(String args[]){
		List<ZkClient> zkClients = new ArrayList<ZkClient>();
		List<WorkServer> workServers = new ArrayList<WorkServer>();
		
		try{
			for(int i = 0; i < ZOOKEEPER_CLIENTS; i++){
				ZkClient client = new ZkClient(ZOOKEEPER_SERVER_ADDRESS, 5000, 5000, new SerializableSerializer());
				zkClients.add(client);
				
				ServerInfo serverInfo = new ServerInfo();
				serverInfo.setId(1000000+(i+1));
				serverInfo.setName("Work Server #" + "192.168.1." + (i+1));
				serverInfo.setIpAddress("192.168.1."+(i+1));
				
				WorkServer ws = new WorkServer(serverInfo, client);
				
				workServers.add(ws);
				ws.serverStart();
			}
			
			
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			log.info("*******************Shutting down*******************");
			for(WorkServer ws : workServers){
				try {
					ws.serverStop();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			for(ZkClient client : zkClients){
				client.close();
			}
		}
		
	}
}
