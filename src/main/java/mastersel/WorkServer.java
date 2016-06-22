package mastersel;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Kevin
 *
 */
public class WorkServer implements NodeServer {
	
	private static Logger log = LoggerFactory.getLogger(WorkServer.class);
	
	private boolean isRunning = false;
	
	private ZkClient zkClient;
	
	private ServerInfo serverInfo;
	
	private boolean isMaster = false;
	
	private IZkDataListener masterNodeDataChangeListener;
	private IZkDataListener slaveNodeDataChangeListener;
	
	private ScheduledExecutorService delayElectMasterExecutor = Executors.newScheduledThreadPool(1);
	private ScheduledExecutorService simulateMasterDownExecutor = Executors.newScheduledThreadPool(1);
	ScheduledExecutorService serverRestartExecutor = Executors.newScheduledThreadPool(1); 
	
	
	private static final int delayElectMasterTime = 5;
	private static final int simulateMasterDownTime = 30;
	
	
	public WorkServer(ServerInfo serverInfo, ZkClient zkClient){
		this.serverInfo = serverInfo;
		this.zkClient = zkClient;
		
		boolean slaveExists = zkClient.exists(SLAVES_PATH);
		
		//if slave header not exist, create it
		if(!slaveExists){
			zkClient.create(SLAVES_PATH, "slave_header", CreateMode.PERSISTENT);
			log.info(">>>>>>>>slave header node created!");
		}
		
		
		this.masterNodeDataChangeListener = new IZkDataListener(){

			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
					delayElectMasterExecutor.schedule(new Runnable(){

						@Override
						public void run() {
							electMaster();
						}
						
					} , delayElectMasterTime, TimeUnit.SECONDS);
			}
		};
		
		this.slaveNodeDataChangeListener = new IZkDataListener(){

			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				delayElectMasterExecutor.schedule(new Runnable(){

					@Override
					public void run() {
						electMaster();
					}
					
				} , delayElectMasterTime, TimeUnit.SECONDS);
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
					
			}
		};
		
	}

	public void serverStart() throws Exception {
		log.info(">>>>>>>>>>>>>>>>Server Running status is :" + isRunning);
		
		if(isRunning){
			log.info(">>>>>>>>>>>>>>>>sever has startup....");
			//throw new Exception("server has startup...");
		}else{
			log.info(">>>>>>>>>>>>>>>>Need startup's server is " + (serverInfo == null ? "" : serverInfo.getName()));
			isRunning = true;
			electMaster();
			if(isMaster){
				log.info(">>>>>>>>>>>>>>>>current master is:" + (serverInfo == null ? "": serverInfo.getId() + "-" + serverInfo.getIpAddress()));
			}
			zkClient.subscribeDataChanges(MASTER_PATH, masterNodeDataChangeListener);
			
			simulateServerRestart();
		}
		
		
	}

	public void serverStop() throws Exception{
		if(!isRunning){
			log.info(">>>>>>>>>>>>>>>>Server " + serverInfo.getId() + " has stoped");
			throw new Exception(" Server " + serverInfo.getId() + " has stoped");
		}
		
		isRunning = false;
		isMaster = false;
		delayElectMasterExecutor.shutdown();
		zkClient.unsubscribeDataChanges(MASTER_PATH, masterNodeDataChangeListener);
		
		releaseMaster();

	}
	
	private void electMaster(){
		if(!isRunning){
			return;
		}
		
		boolean masterExists = zkClient.exists(MASTER_PATH);
		
		//if master not exist , create master use current server, check the slaves, 
		//if slaves has current server, remove current server from slaves
		if(!masterExists){
			zkClient.create(MASTER_PATH, serverInfo, CreateMode.EPHEMERAL);
			this.isMaster = true;
			log.info(">>>>>>>>>>>>>>>>" + serverInfo.getId() + "-" + serverInfo.getIpAddress() + " is master");
			
			boolean slaveChildNodeExists = zkClient.exists(SLAVES_PATH + "/" + serverInfo.getId());
			if(slaveChildNodeExists){
				log.info(">>>>>>>>>>>>>>>>>delete " + SLAVES_PATH + "/" + serverInfo.getId() + " from slaves");
				zkClient.delete(SLAVES_PATH + "/" + serverInfo.getId());
			}
			
		}else{//master exists, set current server as slave
			ServerInfo masterInfo = zkClient.readData(MASTER_PATH);
			if(masterInfo != null && masterInfo.getId() == serverInfo.getId()){
				this.isMaster = true;
			}
			
			//master exists, create this serverinfo as slave child node
			boolean slaveChildNodeExists = zkClient.exists(SLAVES_PATH + "/" + serverInfo.getId());
			if(!slaveChildNodeExists){
				zkClient.createEphemeral(SLAVES_PATH + "/" + serverInfo.getId(), serverInfo);
				zkClient.subscribeDataChanges(SLAVES_PATH + "/" + serverInfo.getId(), slaveNodeDataChangeListener);
			}
		}
		
		simulateMasterDown();
	}
	
	/**
	 * simulate the master down, and trigger the master switch
	 */
	public void simulateMasterDown(){
		simulateMasterDownExecutor.schedule(new Runnable() {			
			public void run() {
				if (checkCurrentServerWhetherMaster()){
					releaseMaster();
				}
			}
		}, simulateMasterDownTime, TimeUnit.SECONDS);
	}
	
	
	/**
	 * simulate server restart, put the current server into slaves node
	 */
	public void simulateServerRestart(){
		try{
			serverRestartExecutor.scheduleWithFixedDelay(new Runnable(){
				public void run() {
					if(!isRunning){
						//check current server whether exist in slaves, if not , add it into slaves, the sever also can not been master
						boolean isExistUnderSlaves = zkClient.exists(SLAVES_PATH + "/" + serverInfo.getId());
						if(zkClient.exists(MASTER_PATH)){
							ServerInfo maserServerInfo = zkClient.readData(MASTER_PATH);
							log.info("***********restart check, master is :" + maserServerInfo.getId() + " restarting server is :" + serverInfo.getId());
							if(!isExistUnderSlaves && maserServerInfo.getId() != serverInfo.getId()){
								log.info("************restart server :" + serverInfo.getId() + "#"+serverInfo.getIpAddress());
								zkClient.createEphemeral(SLAVES_PATH + "/" + serverInfo.getId(), serverInfo);
								zkClient.subscribeDataChanges(SLAVES_PATH + "/" + serverInfo.getId(), slaveNodeDataChangeListener);
							}
						}
					}
				}
			}, 3, 3, TimeUnit.SECONDS); 
		}catch(Exception e){
			log.info("*****************restart server Error " + e.getMessage());
		}
		
	}
	
	/**
	 * if current server is master, delete master node
	 */
	private void releaseMaster() {
		if(checkCurrentServerWhetherMaster()){
			ServerInfo delMaserInfo = zkClient.readData(MASTER_PATH);
			log.info(">>>>>>>>>>>>>release master server is : " + delMaserInfo.getId() + "#" + delMaserInfo.getIpAddress());
			zkClient.delete(MASTER_PATH);
			this.isMaster = false;
			this.isRunning = false;
		}
		
	}

	private boolean checkCurrentServerWhetherMaster(){
		try{
			if(zkClient.exists(MASTER_PATH)){
				ServerInfo currentMasterNodeInfo = zkClient.readData(MASTER_PATH);
				if(currentMasterNodeInfo.getId() == serverInfo.getId()){
					return true;
				}
			}
			return false;
		} catch(ZkNoNodeException e){
			log.info(">>>>>>>>>>>>>>>>No Node Exception :" + e.getMessage());
			return false;
		} catch(ZkInterruptedException e){
			log.info(">>>>>>>>>>>>>>>>Interrrupted Exception :" + e.getMessage());
			return checkCurrentServerWhetherMaster();
		} catch(ZkException e){
			log.info(">>>>>>>>>>>>>>>>Zk Exception :" + e.getMessage());
			return false;
		}
	}

}
