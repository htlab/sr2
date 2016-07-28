package soxrecorderv2.finder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;

import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.logging.SR2LogType;
import soxrecorderv2.logging.SR2Logger;
import soxrecorderv2.recorder.Recorder;
import soxrecorderv2.recorder.RecorderSubProcess;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.SQLUtil;
import soxrecorderv2.util.SR2DatabaseUtil;
import soxrecorderv2.util.ThreadUtil;

/**
 * 定期的にXMPPでノードリストを取得し, 存在していなかったら observation に登録する。
 * @author tomotaka
 *
 */
public class Finder implements Runnable, RecorderSubProcess {
	
	public static final long DEFAULT_INTERVAL_MSEC = 1000 * 60 * 10;  // 10minutes
	
	private Recorder parent;
	
	private String soxServer;
	private long intervalSec = DEFAULT_INTERVAL_MSEC;
	private volatile boolean isRunning;
	private PGConnectionManager connManager;
	private SR2Logger logger;
	
	public Finder(Recorder parent, String soxServer) {
		this.parent = parent;
		this.logger = parent.createLogger(getComponentName());
		this.soxServer = soxServer;
		this.connManager = new PGConnectionManager(parent.getConfig());
	}

	@Override
	public void run() {
		logger.info(SR2LogType.FINDER_START, "finder start", soxServer);
		System.out.println("[Finder][" + soxServer + "] run() start");
		isRunning = true;
		
		while (isRunning) {
			long tBefore = System.currentTimeMillis();
			List<NodeIdentifier> currentExistingNodes;
			System.out.println("[Finder][" + soxServer + "] going to fetch nodes");
			try {
				currentExistingNodes = retrieveNodes();
				System.out.println("[Finder][" + soxServer + "] got node list, N=" + currentExistingNodes.size());
			} catch (SmackException | IOException | XMPPException e1) {
				logger.error(SR2LogType.JAVA_GENERAL_EXCEPTION, "unexpected exception in retrieveNodes() in Finder", e1);
				continue;
			}
			
			Set<String> blacklist;
			try {
				blacklist = getBlacklist();
			} catch (SQLException e2) {
				logger.error(SR2LogType.JAVA_SQL_EXCEPTION, "SQL exception during blacklist fetch", e2);
				continue;
			}
		
			Set<String> nodesInDatabase = null;
			try {
				nodesInDatabase = getNodesInDatabase();
			} catch (SQLException e1) {
				logger.error(SR2LogType.JAVA_SQL_EXCEPTION, "SQL exception during getNodesInDatabase() in Finder", e1);
				continue;
			}
			
			// SOX-Recorderに登録されてないものを, 登録する
			for (NodeIdentifier nodeId : currentExistingNodes) {
				// FIXME ここでキャッシュを使っていちいちpostgresに問い合わせるコストを減らすべき
				if (blacklist.contains(nodeId.getNode())) {
					continue;
				}
				if (nodesInDatabase.contains(nodeId.getNode())) {
					continue;
				}
				try {
					// DBに存在していなかった&&DBに登録した: subscribeする
					register(nodeId);
					parent.subscribe(nodeId);
					logger.info(SR2LogType.OBSERVATION_CREATE, "create by finder", soxServer, nodeId.getNode());
				} catch (SQLException e) {
					logger.error(SR2LogType.OBSERVATION_CREATE_FAILED, "create failed by finder", soxServer, nodeId.getNode(), e);
					continue;
				}
			}
			
			System.out.println("[Finder][" + soxServer + "] sleeping...");
			while (isRunning && (System.currentTimeMillis() - tBefore < intervalSec)) {
				ThreadUtil.sleep(250);
			}
			System.out.println("[Finder][" + soxServer + "] sleep finished.");
		}
		System.out.println("[Finder][" + soxServer + "] END OF run()");
		connManager.close();
		logger.info(SR2LogType.FINDER_STOP, "finder stopped", soxServer);
	}

	@Override
	public void shutdownSubProcess() {
		System.out.println("[Finder][" + soxServer + "] @@@@@ shutdown 1");
		isRunning = false;
		System.out.println("[Finder][" + soxServer + "] @@@@@ shutdown 2 finished");
	}
	
	private Set<String> getBlacklist() throws SQLException {
		Connection conn = connManager.getConnection();
		Set<String> ret = SR2DatabaseUtil.getBlacklistNodes(conn, soxServer);
		connManager.updateLastCommunicateTime();
		return ret;
	}

	private List<NodeIdentifier> retrieveNodes() throws SmackException, IOException, XMPPException {
		// this.soxServerにXMPP接続して, ノードのリストを取得する。
//		List<String> nodes = SOXUtil.fetchAllSensors(parent.getDefaultRecorderLoginInfo(soxServer));
		List<String> nodes = SOXUtil.fetchAllSensors(soxServer); // FIXME use anonymous to avoid conflict(is this right?)
		List<NodeIdentifier> ret = new ArrayList<>();
		for (String node : nodes) {
			ret.add(new NodeIdentifier(soxServer, node));
		}
		return ret;
	}
	
	/**
	 * 担当しているsoxServerにひもづいてDBに保存されているすべてのノードのSetを返す
	 * @return
	 * @throws SQLException
	 */
	private Set<String> getNodesInDatabase() throws SQLException {
		Connection conn = connManager.getConnection();
		String sql = "SELECT sox_node FROM observation WHERE sox_server = ?;";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, soxServer);
		ResultSet rs = ps.executeQuery();
		Set<String> ret = new HashSet<>();
		while (rs.next()) {
			ret.add(rs.getString(1));
		}
		rs.close();
		ps.close();
		return ret;
	}
	
	/**
	 * postgresのobservationテーブルに登録する
	 * @param conn
	 * @param nodeId
	 * @throws SQLException
	 */
	private void register(NodeIdentifier nodeId) throws SQLException {
//		System.out.println("[Finder][" + soxServer + "][register] 1 going to register node=" + nodeId.getNode());
		String[] fields = {
			"sox_server",            // 1
			"sox_node",              // 2
			"sox_jid",               // 3
			"sox_password",          // 4
			"is_using_default_user", // 5
			"is_existing",           // 6
			"is_record_stopped",     // 7
			"is_subscribed",         // 8
			"is_subscribe_failed",   // 9
			"created"                // 10
		};
		Connection conn = connManager.getConnection();
		String sql = SQLUtil.buildInsertSql(SR2Tables.Observation, fields);
//		System.out.println("[Finder][" + soxServer + "][register] 2 built sql node=" + nodeId.getNode());
		
		Savepoint savePointBeforeRegister = conn.setSavepoint();  // use transaction (connection is not auto commit mode)
//		System.out.println("[Finder][" + soxServer + "][register] 3 set savepoint node=" + nodeId.getNode());
		boolean problem = false;
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
//			System.out.println("[Finder][" + soxServer + "][register] 4 prepared sql node=" + nodeId.getNode());
			
			ps.setString(1, nodeId.getServer());               // 1 sox_server
			ps.setString(2, nodeId.getNode());                 // 2 sox_node
			ps.setNull(3, Types.VARCHAR);                      // 3 sox_jid
			ps.setNull(4, Types.VARCHAR);                      // 4 sox_password
			ps.setBoolean(5, true);                            // 5 is_using_default_value
			ps.setBoolean(6, true);                            // 6 is_existing
			ps.setBoolean(7, false);                           // 7 is_record_stopped
			ps.setBoolean(8, false);                           // 8 is_subscribed
			ps.setBoolean(9, false);                           // 9 is_subscribe_failed
			ps.setTimestamp(10, SQLUtil.getCurrentTimestamp()); // 10 created
//			System.out.println("[Finder][" + soxServer + "][register] 5 before exec node=" + nodeId.getNode());
			
			int affectedRows = ps.executeUpdate();
//			System.out.println("[Finder][" + soxServer + "][register] 6 after exec node=" + nodeId.getNode());
			connManager.updateLastCommunicateTime();
			if (affectedRows != 1) {
				// FIXME なにかがおかしい
				System.out.println("[Finder][" + soxServer + "][register] ERROR: could not register, node=" + nodeId.getNode());
			} else {
//				System.out.println("[Finder][" + soxServer + "][register] registered, node=" + nodeId.getNode());
			}
			ps.close();
		} catch (Exception e) {
			logger.error(SR2LogType.JAVA_SQL_EXCEPTION, "uncaught exception in register()", e);
			problem = true;
		} finally {
			if (problem) {
				conn.rollback(savePointBeforeRegister);
			} else {
				conn.commit();
			}
		}
	}

	@Override
	public PGConnectionManager getConnManager() {
		return connManager;
	}

	@Override
	public String getComponentName() {
		return "finder";
	}

}
