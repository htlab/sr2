package soxrecorderv2.recorder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smackx.pubsub.PubSubManager;
import org.jivesoftware.smackx.pubsub.Subscription;

import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;
import jp.ac.keio.sfc.ht.sox.soxlib.SoxDevice;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.common.model.SoxLoginInfo;
import soxrecorderv2.logging.SR2LogType;
import soxrecorderv2.logging.SR2Logger;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.SQLUtil;

/**
 * NOTE: this component is not supposed to be run as a thread.
 * @author tomotaka
 *
 */
public class SubStateSynchronizer implements Runnable, RecorderSubProcess {
	
	@SuppressWarnings("unused")
	private Recorder parent;
	private SoxLoginInfo soxLoginInfo;
	private PGConnectionManager pgConnManager;
	private SR2Logger logger;
	
	public SubStateSynchronizer(Recorder parent, SoxLoginInfo soxLoginInfo) {
		this.parent = parent;
		this.logger = parent.createLogger(getComponentName());
		this.soxLoginInfo = soxLoginInfo;
		this.pgConnManager = new PGConnectionManager(parent.getConfig());
	}

	public String getSoxServer() {
		return soxLoginInfo.getServer();
	}
	
	@Override
	public void run() {
		logger.info(SR2LogType.STATE_SYNCHRONIZER_START, "sub-state synchronizer start", getSoxServer());
		debug("start");
		SoxConnection conn = null;
		try {
			conn = SOXUtil.open(soxLoginInfo);
			
			// いまsubscribeしてるリストを取得
			PubSubManager pubSubManager = conn.getPubSubManager();
			List<Subscription> subscriptions = pubSubManager.getSubscriptions();
			Set<String> subscribedSoxNodes = new HashSet<>();
			for (Subscription sub : subscriptions) {
				String nodeName = sub.getNode();
				if (nodeName.endsWith("_data")) {
					String soxNodeName = nodeName.substring(0, nodeName.length() - 5);
					subscribedSoxNodes.add(soxNodeName);
				}
			}
			debug("found " + subscribedSoxNodes.size() + " nodes in subscription");
			
			// DBにある, このSubStateSynchronizerが担当しているserverのnode listを取得
			Set<String> soxNodesInDatabase = getSoxNodesInDatabase(getSoxServer());
			debug("found " + soxNodesInDatabase.size() + " nodes in database");
			
			// すでにsubしていて, dbにないもの: observationをつくる
			Set<String> subscribedButNoObservation = SetUtils.difference(subscribedSoxNodes, soxNodesInDatabase).toSet();
			if (0 < subscribedButNoObservation.size()) {
				debug("going to create observation: N=" + subscribedButNoObservation.size());
				createObservations(subscribedButNoObservation);
			} else {
				debug("no need to create observation");
			}
		
			// DBにあってsubしていないもの: subする
			Set<String> soxNodesInDatabaseButNotSubscribed = SetUtils.difference(soxNodesInDatabase, subscribedSoxNodes).toSet();
			if (0 < soxNodesInDatabase.size()) {
				debug("going to subscribe: N=" + soxNodesInDatabase.size());
				subscribeAll(conn, soxNodesInDatabaseButNotSubscribed);
			} else {
				debug("no need to subscribe");
			}
		} catch (SmackException | IOException | XMPPException e) {
			e.printStackTrace(); // TODO
		} catch (SQLException e) {
			e.printStackTrace(); // TODO
		} catch (Exception e) {
			e.printStackTrace(); // TODO
		} finally {
			SOXUtil.closeQueitly(conn);
			pgConnManager.close();
		}
		debug("finished");
		logger.info(SR2LogType.STATE_SYNCHRONIZER_STOP, "sub-state synchronizer stop", getSoxServer());
	}
	
	private void createObservations(Collection<String> soxNodeNames) throws SQLException {
		for (String soxNodeName : soxNodeNames) {
			createObservation(soxNodeName);
		}
	}
	
	private void subscribeAll(SoxConnection conn, Collection<String> soxNodeNames) throws Exception {
		for (String soxNodeName : soxNodeNames) {
			subscribe(conn, soxNodeName);
		}
	}
	
	private Set<String> getSoxNodesInDatabase(String soxServer) throws SQLException {
		Connection pgConn = pgConnManager.getConnection();
		Set<String> ret = new HashSet<>();
		
		String sql = "SELECT sox_node FROM observation WHERE sox_server = ?;";
		PreparedStatement ps = pgConn.prepareStatement(sql);
		ps.setString(1, getSoxServer());
		
		ResultSet rs = ps.executeQuery();
		pgConnManager.updateLastCommunicateTime();
		while (rs.next()) {
			String soxNodeName = rs.getString(1);
			ret.add(soxNodeName);
		}
		rs.close();
		ps.close();
		
		return ret;
	}
	
	/**
	 * 
	 * @param conn
	 * @param soxNodeName does not need trailing "_data"
	 * @throws SQLException 
	 */
	private void createObservation(String soxNodeName) throws SQLException {
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
		Connection conn = pgConnManager.getConnection();
		String sql = SQLUtil.buildInsertSql(SR2Tables.Observation, fields);
		
		Savepoint savePointBeforeRegister = conn.setSavepoint();  // use transaction (connection is not auto commit mode)
		boolean problem = false;
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			
			ps.setString(1, getSoxServer()); // sox_server
			ps.setString(2, soxNodeName);   // sox_node
			ps.setNull(3, Types.VARCHAR);        // sox_jid
			ps.setNull(4, Types.VARCHAR);        // sox_password
			ps.setBoolean(5, true);              // is_using_default_value
			ps.setBoolean(6, true);              // is_existing
			ps.setBoolean(7, false);             // is_record_stopped
			ps.setBoolean(8, false);             // is_subscribed
			
			ps.setTimestamp(9, SQLUtil.getCurrentTimestamp());
			
			int affectedRows = ps.executeUpdate();
			pgConnManager.updateLastCommunicateTime();
			if (affectedRows != 1) {
				// FIXME なにかがおかしい
				debug("[register] failed to register: " + soxNodeName);
			} else {
			}
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
			problem = true;
		} finally {
			if (problem) {
				conn.rollback(savePointBeforeRegister);
				debug("rollbacked! node=" + soxNodeName);
			} else {
				conn.commit();
			}
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @param soxNodeName does not need trailing "_data"
	 * @throws Exception 
	 */
	private void subscribe(SoxConnection conn, String soxNodeName) throws Exception {
		SoxDevice soxDevice = new SoxDevice(conn, soxNodeName);
		soxDevice.subscribe();
	}

	@Override
	public void shutdownSubProcess() {
		pgConnManager.close();
	}

	@Override
	public PGConnectionManager getConnManager() {
		return pgConnManager;
	}
	
	private void debug(String msg) {
		System.out.println("[Synchronizer][" + getSoxServer() + "] " + msg);
	}

	@Override
	public String getComponentName() {
		return "sub_state_synchronizer";
	}

}
