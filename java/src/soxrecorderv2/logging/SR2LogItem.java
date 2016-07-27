package soxrecorderv2.logging;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class SR2LogItem {
	
	private final String system;
	private final String component;
	private final String soxServer;
	private final String soxNode;
	private final String message;
	private final SR2LogLevel logLevel;
	private final SR2LogType logType;
	private final Timestamp loggedAt;
	
	public SR2LogItem(
			String system, String component, String soxServer, String soxNode, String message,
			SR2LogLevel logLevel, SR2LogType logType, Timestamp loggedAt) {
		this.system = system;
		this.component = component;
		this.soxServer = soxServer;
		this.soxNode = soxNode;
		this.message = message;
		this.logLevel = logLevel;
		this.logType = logType;
		this.loggedAt = loggedAt;
	}
	
	public void fillPreparedStatement(PreparedStatement ps) throws SQLException {
		ps.setString(1, system);
		ps.setString(2, component);
		if (soxServer == null) {
			ps.setNull(3, Types.VARCHAR);
		} else {
			ps.setString(3, soxServer);
		}
		if (soxNode == null) {
			ps.setNull(4, Types.VARCHAR);
		} else {
			ps.setString(4, soxNode);
		}
		ps.setString(5, message);
		ps.setInt(6, logLevel.getLevel());
		ps.setInt(7, logType.getNumber());
		ps.setTimestamp(8, loggedAt);
	}

	public String getSystem() {
		return system;
	}

	public String getComponent() {
		return component;
	}

	public String getSoxServer() {
		return soxServer;
	}

	public String getSoxNode() {
		return soxNode;
	}

	public String getMessage() {
		return message;
	}

	public SR2LogLevel getLogLevel() {
		return logLevel;
	}

	public SR2LogType getLogType() {
		return logType;
	}

	public Timestamp getLoggedAt() {
		return loggedAt;
	}

}
