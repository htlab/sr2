package soxrecorderv2.common.model;

public class SoxLoginInfo {
	
	private String server;
	private String jid;
	private String password;
	private boolean isAnonymous;

	public SoxLoginInfo(String server) {
		this.server = server;
		this.jid = null;
		this.password = null;
		this.isAnonymous = true;
	}
	
	public SoxLoginInfo(String server, String jid, String password) {
		this.server = server;
		this.jid = jid;
		this.password = password;
		this.isAnonymous = false;
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
	public boolean isAnonymous() {
		return isAnonymous;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public void setJid(String jid) {
		this.jid = jid;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public void setAnonymous(boolean isAnonymous) {
		this.isAnonymous = isAnonymous;
	}

}
