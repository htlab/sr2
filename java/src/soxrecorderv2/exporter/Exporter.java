package soxrecorderv2.exporter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.arnx.jsonic.JSONException;
import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.ExportingState;
import soxrecorderv2.common.model.db.Export;
import soxrecorderv2.util.PGConnectionManager;

/**
 * runs as a daemon, waiting for request to build export archive
 * @author tomotaka
 *
 */
public class Exporter implements Runnable {
	
	public static final long QUEUE_PUT_TIMEOUT_MSEC = 100;
	
	public static final String CONFIG_KEY_ENDPOINT = "controller_endpoint";
	public static final String CONFIG_KEY_EXPORTER_API_KEY = "exporter_api_key";
	
	private String configFile;
//	private SR2SystemConfig config;
	private Properties config;
	private PGConnectionManager connManager;
	
	private LinkedBlockingQueue<ExportTask> taskQueue;
	private ExportThread exportThread;
//	private SoxRecorderClient client;
	private boolean isRunning;
	
	public Exporter(String configFile) throws JSONException, IOException {
		this.configFile = configFile;
//		this.config = new SR2SystemConfig(configFile);
		loadConfig();
		connManager = new PGConnectionManager(config);
	}
	
	public LinkedBlockingQueue<ExportTask> getTaskQueue() {
		return taskQueue;
	}
	
	@Override
	public void run() {
		isRunning = true;
		taskQueue = new LinkedBlockingQueue<>();
		
		// start export thread
		exportThread = new ExportThread(this);
		(new Thread(exportThread)).run();
	}
	
	public boolean isRunning() {
		return isRunning;
	}
	
	public PGConnectionManager getConnManager() {
		return connManager;
	}
	
	protected void enqueueExportTask(long exportId) {
		// TODO: fetch export profile
		Export export = null;
		
		ExportingState newState = ExportingState.IN_QUEUE;
		ExportTask task = new ExportTask(newState, export);
		
		boolean putFinished = false;
		while (!putFinished && isRunning) {
			try {
				taskQueue.offer(task, QUEUE_PUT_TIMEOUT_MSEC, TimeUnit.MILLISECONDS);
				putFinished = true;
			} catch (InterruptedException e) {
				// TODO: logging
			}
		}
		
//		getClient().updateExportState(exportId, newState);
	}
	
//	protected SoxRecorderClient getClient() {
//		if (client == null) {
//			client = buildClient();
//		}
//		return client;
//	}
	
//	protected SoxRecorderClient buildClient() {
//		String endpoint = config.get("controller_endpoint");
//		String apiKey = config.get("exporter_api_key");
//		
//		return new SoxRecorderClient(endpoint, apiKey);
//	}
	
	private void loadConfig() {
		try (FileInputStream fin = new FileInputStream(configFile)) {
			config = new Properties();
			config.load(fin);
			System.out.println("[Recorder] loaded properties(config) from file=" + configFile);
		} catch (FileNotFoundException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws JSONException, IOException {
		if (args.length == 0) {
			throw new IllegalArgumentException("missing config file in argument");
		}
		String configFile = args[0];
		
		final Exporter exporter = new Exporter(configFile);
		// TODO: register SIGINT handler to stop exporter
		exporter.run();
	}

}
