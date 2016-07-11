package soxrecorderv2.finder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;

import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.recorder.Recorder;
import soxrecorderv2.recorder.RecorderSubProcess;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.SQLUtil;
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
	private boolean isRunning;
	private PGConnectionManager connManager;
	
	public Finder(Recorder parent, String soxServer) {
		this.parent = parent;
		this.soxServer = soxServer;
		this.connManager = new PGConnectionManager(parent.getConfig());
	}

	@Override
	public void run() {
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
				// Auto-generated catch block
				e1.printStackTrace();
				continue;
			}
			
			// SOX-Recorderに登録されてないものを, 登録する
			for (NodeIdentifier nodeId : currentExistingNodes) {
				// FIXME ここでキャッシュを使っていちいちpostgresに問い合わせるコストを減らすべき
				try {
					boolean isNewlyRegistered = registerIfNotExist(nodeId);
					if (isNewlyRegistered) {
						// DBに存在していなかった&&DBに登録した: subscribeする
						parent.subscribe(nodeId);
					}
				} catch (SQLException e) {
					// Auto-generated catch block
					e.printStackTrace();
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
	}

	@Override
	public void shutdownSubProcess() {
		System.out.println("[Finder][" + soxServer + "] @@@@@ shutdown 1");
		isRunning = false;
		System.out.println("[Finder][" + soxServer + "] @@@@@ shutdown 2 finished");
	}

	private List<NodeIdentifier>  retrieveNodes() throws SmackException, IOException, XMPPException {
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
	 * 
	 * @param nodeId
	 * @return true if registered
	 * @throws SQLException
	 */
	private boolean registerIfNotExist(NodeIdentifier nodeId) throws SQLException {
		if (!isExists(nodeId)) {
			register(nodeId);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * postgresのobservationテーブルに存在しているかチェックする
	 * @param conn
	 * @param nodeId
	 * @return
	 * @throws SQLException
	 */
	private boolean isExists(NodeIdentifier nodeId) throws SQLException {
		Connection conn = connManager.getConnection();
		String sql = "SELECT id FROM observation WHERE sox_server = ? AND sox_node = ?;";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, nodeId.getServer());
		ps.setString(2, nodeId.getNode());
		
		ResultSet rs = ps.executeQuery();
		if (!rs.next()) {
//			System.out.println("[Finder][" + soxServer + "][isExists] NOT found, node=" + nodeId.getNode());
			rs.close();
			ps.close();
			return false;
		} else {
//			System.out.println("[Finder][" + soxServer + "][isExists] found, node=" + nodeId.getNode());
			rs.close();
			ps.close();
			return true;
		}
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
			"created"                // 9
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
			
			ps.setString(1, nodeId.getServer()); // sox_server
			ps.setString(2, nodeId.getNode());   // sox_node
			ps.setNull(3, Types.VARCHAR);        // sox_jid
			ps.setNull(4, Types.VARCHAR);        // sox_password
			ps.setBoolean(5, true);              // is_using_default_value
			ps.setBoolean(6, true);              // is_existing
			ps.setBoolean(7, false);             // is_record_stopped
			ps.setBoolean(8, false);             // is_subscribed
			
			ps.setTimestamp(9, SQLUtil.getCurrentTimestamp());
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
			e.printStackTrace();
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

}
