package mastersel;

public interface NodeServer {
	
	public static final String MASTER_PATH = "/master"; 
	public static final String SLAVES_PATH = "/slaves";
	
	public void serverStart() throws Exception;
	
	public void serverStop() throws Exception;

}
