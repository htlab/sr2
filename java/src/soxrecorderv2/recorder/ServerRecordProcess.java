package soxrecorderv2.recorder;


/**
 * Recorder main thread runs this ServerRecordProcess instances for each SOX Server.
 * (SOX server list is built by)
 * @author tomotaka
 *
 */
public class ServerRecordProcess implements Runnable {
	
	private final String soxServer;
	
	public ServerRecordProcess(String soxServer) {
		this.soxServer = soxServer;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stubo

	}

}
