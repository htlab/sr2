package soxrecorderv2.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;

public class PGConnectionManager {

	public static final String CONFIG_KEY_PG_HOST   = "pg_host";
	public static final String CONFIG_KEY_PG_DBNAME = "pg_dbname";
	public static final String CONFIG_KEY_PG_USER   = "pg_user";
	public static final String CONFIG_KEY_PG_PASS   = "pg_pass";
	
	private Properties config;

	private Connection conn = null;
	private long connectionResetPeriod = 1000 * 60 * 10; // 10minutes
	private long lastCommunicatedAt = 0;
	
	public PGConnectionManager(Properties config) {
//		this.config = parent.getConfig();
		this.config = config;
	}
	
	public Connection getConnection() throws SQLException {
		// memo: http://mountainbigroad.jp/fc5/pgsql_java.html
		if (conn == null && connectionResetPeriod < getNow() - lastCommunicatedAt) {
//			System.out.println("[PGConnectionManager][getConnection] conn is null or cache expired, going to newly open");
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
			
			conn = openConnection();
			updateLastCommunicateTime();
		} else {
//			System.out.println("[PGConnectionManager][getConnection] going to use cached conn");
		}
		return conn;
	}

	private Connection openConnection() throws SQLException {
//		System.out.println("[PGConnectionManager](0) going to open connection to PostgreSQL");
		String pgHost     = config.getProperty(CONFIG_KEY_PG_HOST);
		String pgDatabase = config.getProperty(CONFIG_KEY_PG_DBNAME);
		String pgUser     = config.getProperty(CONFIG_KEY_PG_USER);
		String pgPass     = config.getProperty(CONFIG_KEY_PG_PASS);
		String url = "jdbc:postgresql://" + pgHost + "/" + pgDatabase;
//		System.out.println("[PGConnectionManager](1): host=" + pgHost + ", db=" + pgDatabase + ", user=" + pgUser + ", pass=" + pgPass);
//		System.out.println("[PGConnectionManager](2): url=" + url);
		Connection conn = DriverManager.getConnection(url, pgUser, pgPass);
		conn.setAutoCommit(false);
		return conn;
	}
	
	public void updateLastCommunicateTime() {
		lastCommunicatedAt = getNow();
	}

	/**
	 * 
	 * @return epoch秒をミリ秒で返す
	 */
	private long getNow() {
		return Calendar.getInstance().getTimeInMillis();
	}
	
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// FIXME Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
