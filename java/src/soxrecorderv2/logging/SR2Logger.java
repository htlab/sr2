package soxrecorderv2.logging;

import java.sql.Timestamp;
import java.util.concurrent.LinkedBlockingQueue;

import soxrecorderv2.util.SQLUtil;

public class SR2Logger {
	
	public static final String SYSTEM = "recorder";
	
	private String system;
	private String component;
	private LinkedBlockingQueue<SR2LogItem> logDrain;
	
	public SR2Logger(String component, LinkedBlockingQueue<SR2LogItem> logDrain) {
		this(SYSTEM, component, logDrain);
	}
	
	public SR2Logger(String system, String component, LinkedBlockingQueue<SR2LogItem> logDrain) {
		this.system = system;
		this.component = component;
		this.logDrain = logDrain;
	}
	
	public void debug(SR2LogType type, String message) {
		debug(type, message, null, null);
	}
	
	public void debug(SR2LogType type, String message, String soxServer) {
		debug(type, message, soxServer, null);
	}
	
	public void debug(SR2LogType type, String message, String soxServer, String soxNode) {
		log(SR2LogLevel.DEBUG, type, message, soxServer, soxNode);
	}
	
	public void info(SR2LogType type, String message) {
		info(type, message, null, null);
	}
	
	public void info(SR2LogType type, String message, String soxServer) {
		info(type, message, soxServer, null);
	}
	
	public void info(SR2LogType type, String message, String soxServer, String soxNode) {
		log(SR2LogLevel.INFO, type, message, soxServer, soxNode);
	}

	public void warn(SR2LogType type, String message) {
		warn(type, message, null, null, null);
	}

	public void warn(SR2LogType type, String message, Throwable exception) {
		warn(type, message, null, null, exception);
	}
	
	public void warn(SR2LogType type, String message, String soxServer) {
		warn(type, message, soxServer, null, null);
	}

	public void warn(SR2LogType type, String message, String soxServer, String soxNode) {
		log(SR2LogLevel.WARNING, type, message, soxServer, soxNode, null);
	}
	
	public void warn(SR2LogType type, String message, String soxServer, String soxNode, Throwable exception) {
		log(SR2LogLevel.WARNING, type, message, soxServer, soxNode, exception);
	}

	public void error(SR2LogType type, String message) {
		error(type, message, null, null, null);
	}
	
	public void error(SR2LogType type, String message, Throwable exception) {
		error(type, message, null, null, exception);
	}
	
	public void error(SR2LogType type, String message, String soxServer) {
		error(type, message, soxServer, null, null);
	}

	public void error(SR2LogType type, String message, String soxServer, String soxNode) {
		error(type, message, soxServer, soxNode, null);
	}
	
	public void error(SR2LogType type, String message, String soxServer, String soxNode, Throwable exception) {
		log(SR2LogLevel.ERROR, type, message, soxServer, soxNode, exception);
	}
	
	public void log(SR2LogLevel level, SR2LogType type, String message) {
		log(level, type, message, null, null);
	}
	
	public void log(SR2LogLevel level, SR2LogType type, String message, String soxServer) {
		log(level, type, message, soxServer, null);
	}

	public void log(SR2LogLevel level, SR2LogType type, String message, String soxServer, String soxNode) {
		log(level, type, message, soxServer, soxNode, null);
	}

	public void log(SR2LogLevel level, SR2LogType type, String message, String soxServer, String soxNode, Throwable exception) {
		Timestamp now = SQLUtil.getCurrentTimestamp();
		SR2LogItem logItem = new SR2LogItem(system, component, soxServer, soxNode, message, level, type, now);
		
		if (exception != null) {
			logItem.setException(exception);
		}
		
		try {
			logDrain.put(logItem);
		} catch (InterruptedException e) {
			e.printStackTrace(); // TODO
		}
	}
	
	public String getSystem() {
		return system;
	}
	
	public String getComponent() {
		return component;
	}
	
	public LinkedBlockingQueue<SR2LogItem> getLogDrain() {
		return logDrain;
	}

}
