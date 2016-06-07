package soxrecorderv2.common.model;

/**
 * observation connection info
 * @author tomotaka
 *
 */
public class ObConnInfo {
	
	private final String server;
	private final String jid;
	private final String password;
	
	/**
	 * create anonymous ObConnInfo
	 * @param server
	 */
	public ObConnInfo(String server) {
		this(server, null, null);
	}
	
	public ObConnInfo(String server, String jid, String password) {
		this.server = server;
		this.jid = jid;
		this.password = password;
	}
	
	public boolean isAnonymous() {
		return (jid == null && password == null);
	}
	
	public String getServer() {
		return server;
	}
	
	public String getJid() {
		return jid;
	}
	
	public String getPassword() {
		return password;
	}

}
