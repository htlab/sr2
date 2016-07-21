package soxrecorderv2.recorder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

//import org.dom4j.Document;
//import org.dom4j.DocumentException;
//import org.dom4j.DocumentHelper;
//import org.dom4j.Element;
//import org.dom4j.Node;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.StanzaListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.filter.StanzaTypeFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Stanza;
import org.jivesoftware.smackx.pubsub.LeafNode;
import org.jivesoftware.smackx.pubsub.PubSubManager;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;

import jp.ac.keio.sfc.ht.sox.protocol.Data;
import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;
import jp.ac.keio.sfc.ht.sox.soxlib.SoxDevice;
import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.RecordTask;
import soxrecorderv2.common.model.SoxLoginInfo;
import soxrecorderv2.logging.SR2LogType;
import soxrecorderv2.logging.SR2Logger;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.ThreadUtil;

/**
 * Recorder main thread runs this ServerRecordProcess instances for each SOX Server.
 * (SOX server list is built by)
 * @author tomotaka
 *
 */
public class ServerRecordProcess implements Runnable, RecorderSubProcess {
	
	private static final DocumentBuilder DOC_BUILDER;
	private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();
	private static final String XP_ITEMS = "//items";
	private static final String XP_DATA = "//data";
	
	static {
		DocumentBuilder docBuilder = null;
		
		try {
			docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();  // FIXME
		}
		
		DOC_BUILDER = docBuilder;
	}
	
	/**
	 * SOX server との XMPP コネクションを維持するスレッド。
	 * XMPPコネクションが終了するとスレッドは終了する
	 * @author tomotaka
	 *
	 */
	public class SubscribeThread extends Thread implements StanzaListener {
		
		private volatile boolean isRunning = false;
		private SoxConnection soxConnection = null;
		private boolean isConnected = false;
		private boolean isErrorFinished = false;
		private CountDownLatch connEstablished = new CountDownLatch(1);

		// https://htlab.slack.com/files/takuro/F1NEANLKZ/-.java
		@Override
		public void processPacket(Stanza arg0) throws NotConnectedException {
//			System.out.println("[SRP][" + soxServer + "] received something");
			Message message = (Message) arg0;
			String rawXml = message.toString();
			
			Document doc;
			try {
				synchronized(DOC_BUILDER) {
					doc = DOC_BUILDER.parse(new ByteArrayInputStream(rawXml.getBytes("UTF-8")));
				}
			} catch (SAXException | IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				return;
			}

			/** Get Sensor Name **/
			XPath xpath = XPATH_FACTORY.newXPath();
			Node itemsNode = null;
			try {
				itemsNode = (Node)xpath.evaluate(XP_ITEMS, doc, XPathConstants.NODE);
			} catch (XPathExpressionException e2) {
				// TODO Auto-generated catch block
				System.err.println("@@@ xml error 1");
				e2.printStackTrace();
				return;
			}
			
			String nodeName = itemsNode.getAttributes().getNamedItem("node").getTextContent();
			String sensorName = nodeName.substring(0, nodeName.length() - 5);

			/** Get TransducerValues **/

			// FIXME is this right?
//			dataString = dataString.replaceAll("&lt;", "<");
//			dataString = dataString.replaceAll("/&gt;", ">");
//			dataString = dataString.replaceAll("&apos;", "'");
			
			Node dataNode = null;
			try {
				dataNode = (Node)xpath.evaluate(XP_DATA, doc, XPathConstants.NODE);
			} catch (XPathExpressionException e1) {
				// TODO Auto-generated catch block
				System.err.println("@@@@ xml error 2");
				e1.printStackTrace();
				return;
			}
			DOMImplementationLS ls = (DOMImplementationLS)doc.getImplementation();
			LSSerializer lsSerializer = ls.createLSSerializer();
			
			String dataXml = lsSerializer.writeToString(dataNode);
			
			final Serializer serializer = new Persister();
			Data data = null;
			try {
				data = serializer.read(Data.class, dataXml);
			} catch (Exception e) {
				System.err.println("@@@ 3");
				e.printStackTrace();
				return;  // FIXME
			}
//			System.out.println("[SRP][" + soxServer + "] parsed data");

			NodeIdentifier nodeId = new NodeIdentifier(ServerRecordProcess.this.soxServer, sensorName);
			RecordTask task = new RecordTask(nodeId, data, rawXml);
			
			int putTryCount = 20;  // FIXME
			while (isRunning && 0 < putTryCount--) {
				boolean putSucceeded = ServerRecordProcess.this.taskEmitQueue.offer(task);
				if (putSucceeded) {
//					System.out.println("[SRP][" + soxServer + "] put item to queue, node=" + sensorName);
					break;
				}
				
				ThreadUtil.sleep(1000);
			}
			if (isRunning && putTryCount == 0) {
				System.err.println("[SRP][" + soxServer + "] congestion! gave up to put into queue");
				// FIXME ロギングする
			}
		}
		
		public boolean isConnected() {
			return isConnected;
		}
		
		public boolean isErrorFinished() {
			return isErrorFinished;
		}
		
		public void subscribe(NodeIdentifier nodeId) {
//			System.out.println("[SRP][" + soxServer + "][Sub] going to sub! server=" + nodeId.getServer() + ", node=" + nodeId.getNode());
			if (!isRunning) {
//				System.out.println("[SRP][" + soxServer + "][Sub] not running, returning. server=" + nodeId.getServer() + ", node=" + nodeId.getNode());
				return;
			}
			try {
				// subする
				while (isRunning) {
					// make sure connected to sox server
//					ThreadUtil.countDownLatchAwait(connEstablished, 100);
					try {
						if (connEstablished.await(250, TimeUnit.MILLISECONDS)) {
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if (!isRunning && connEstablished.getCount() != 0) {
					System.out.println("[SRP][" + soxServer + "][Sub] waited subthread's connection, but SRP thread is not running anymore, going to stop subscribe!");
					return;
				}
				boolean subError = false;
//				try {
////					SoxDevice soxDevice = new SoxDevice(soxConnection, nodeId.getNode());
////					soxDevice.subscribe();
//				} catch (Exception e) {
//					e.printStackTrace();
//					subError = true;
//				}
				XMPPConnection xmppConn = soxConnection.getXMPPConnection();
				String xmppUser = xmppConn.getUser();
				PubSubManager pubSubManager = soxConnection.getPubSubManager();
				String dataNodeName = nodeId.getDataNodeName();
				try {
					LeafNode dataNode = pubSubManager.getNode(dataNodeName);
					dataNode.subscribe(xmppUser);
				} catch (Exception e) {
//					e.printStackTrace();
					subError = true;
					logger.warn(SR2LogType.SUBSCRIBE_FAILED, "failed to subscribe", nodeId.getServer(), nodeId.getNode());
				}
				
				System.out.println("[SRP][" + soxServer + "][Sub] subscribed '" + nodeId.getNode() + "'");

				// DBのflagをたてる
				if (subError) {
					markAsSubscribedInDatabase(nodeId);
				} else {
					try {
						markAsSubscribedInDatabase(nodeId);
						System.out.println("[SRP][" + soxServer + "][Sub] marked subscribed '" + nodeId.getNode() + "'");
					} catch (SQLException e) {
						System.err.println("@@@ 1");
						e.printStackTrace();  // FIXME
					}
				}
			} catch (Exception e1) {
				System.err.println("@@@ 2");
				e1.printStackTrace();  // FIXME
			}
		}
		
		public void markAsSubscribeFailed(NodeIdentifier nodeId) throws SQLException {
			Connection conn = connManager.getConnection();
			Savepoint sp = conn.setSavepoint();
			boolean problem = false;
			try {
				String sql = "UPDATE observation SET is_subscribe_failed = ? WHERE sox_server = ? AND sox_node = ?;";
				PreparedStatement ps = conn.prepareStatement(sql);
				ps.setBoolean(1, true);
				ps.setString(2, nodeId.getServer());
				ps.setString(3, nodeId.getNode());
				int affectedRows = ps.executeUpdate();
				connManager.updateLastCommunicateTime();
				if (affectedRows != 1) {
					// FIXME なにかがおかしい
				}
				ps.close();
			} finally {
				if (problem) {
					conn.rollback(sp);
				} else {
					conn.commit();
				}
			}
		}
		
		public void markAsSubscribedInDatabase(NodeIdentifier nodeId) throws SQLException {
			Connection conn = connManager.getConnection();
			Savepoint savePointBeforeMarkAsSubscribe = conn.setSavepoint();  // use transaction
			boolean problem = false;
			try {
				String sql = "UPDATE observation SET is_subscribed = ? WHERE sox_server = ? AND sox_node = ?;";
				PreparedStatement ps = conn.prepareStatement(sql);
				
				ps.setBoolean(1, true);
				ps.setString(2, nodeId.getServer());
				ps.setString(3, nodeId.getNode());
				
				int affectedRows = ps.executeUpdate();
				connManager.updateLastCommunicateTime();
				if (affectedRows != 1) {
					// FIXME なにかがおかしい
				}
				
				ps.close();
			} catch (Exception e) {
				e.printStackTrace();
				problem = true;
			} finally {
				if (problem) {
					conn.rollback(savePointBeforeMarkAsSubscribe);
				} else {
					conn.commit();
				}
			}
		}
		
		@Override
		public void run() {
			isRunning = true;
			SoxLoginInfo soxLoginInfo = ServerRecordProcess.this.getSoxLoginInfo();
			
			// 接続する
			try {
				System.out.println("[SRP][" + soxServer + "][Sub] before open sox conn");
				soxConnection = SOXUtil.open(soxLoginInfo);
				System.out.println("[SRP][" + soxServer + "][Sub] sox conn opened");
				
				// Raw XMPP connectionのAPIをつかう
				XMPPConnection rawXmppConn = soxConnection.getXMPPConnection();
				rawXmppConn.addAsyncStanzaListener(this, new StanzaTypeFilter(Message.class));

				System.out.println("[SRP][" + soxServer + "][Sub] sox conn opened: added stanza listener");
				
				connEstablished.countDown();
				System.out.println("[SRP][" + soxServer + "][Sub] sox conn opened: connEstablished.countDown() called");
			} catch (SmackException | IOException | XMPPException e) {
				e.printStackTrace();
				isErrorFinished = true;
				return;
			}
			isConnected = true;
			// soxConnectionが終了するまでthreadはaliveになる
			
			while (isRunning && soxConnection.getXMPPConnection().isConnected()) {
//				System.out.println("ho ho ho");
				ThreadUtil.sleep(250);
			}
		}
		
		public CountDownLatch getConnEstablished() {
			return connEstablished;
		}
		
		public void stopSubThread() {
			System.out.println("[SRP][" + soxServer + "][Sub] @@@@@ shutdown 1");
			if (soxConnection != null) {
				soxConnection.disconnect();
			}
			System.out.println("[SRP][" + soxServer + "][Sub] @@@@@ shutdown 2");
			isRunning = false;
			System.out.println("[SRP][" + soxServer + "][Sub] @@@@@ shutdown 3 finished");
		}
		
	}
	
	private Recorder parent;
	private final LinkedBlockingQueue<RecordTask> taskEmitQueue;
	private final String soxServer;
	private volatile boolean isRunning;
	private SubscribeThread subThread;
	private CountDownLatch subThreadStarted = null;
	private PGConnectionManager connManager;
	private SR2Logger logger;
	
	private Object subThreadOperationLock = new Object();
	
	public ServerRecordProcess(Recorder parent, LinkedBlockingQueue<RecordTask> taskEmitQueue, String soxServer) {
		this.parent = parent;
		this.logger = parent.createLogger(getComponentName());
		this.taskEmitQueue = taskEmitQueue;
		this.soxServer = soxServer;
		this.connManager = new PGConnectionManager(parent.getConfig());
	}

	@Override
	public void run() {
		logger.info(SR2LogType.RECORD_PROCESS_START, "record process start", soxServer);
		isRunning = true;
		subThread = null;
		
		while (isRunning) {
			System.out.println("[SRP][" + soxServer + "] waiting for sub thread startinig");
			startSubThreadAndWaitConnectionEstablishment();
			System.out.println("[SRP][" + soxServer + "] waiting for sub thread startinig: started!");
			
			// 起動時の処理: dbにsub済みかどうかflagをたてて, flagがないものはsubするみたいなことをする必要がある
			try {
				List<NodeIdentifier> nodesNotSubscribed = getNotSubscribedObservations();  // dbにあるけどsubされてないものをリスト
				System.out.println("[SRP][" + soxServer + "] got nodes not subscribed");
				for (NodeIdentifier nodeId : nodesNotSubscribed) {
//					System.out.println("[SRP][" + soxServer + "] calling subThread.subscribe()");
					subThread.subscribe(nodeId);  // 順番にsub
				}
			} catch (SQLException e) {
				// FIXME
				e.printStackTrace();
				continue;
			}
			
			while (isRunning) {
//				System.out.println("[SRP][" + soxServer + "] sleeping");
				ThreadUtil.sleep(250);
				
				// subthreadが死んだら復活させる
				if (isRunning && !subThread.isAlive()) {
					System.out.println("[SRP][" + soxServer + "] subThread is not alive!");
					renewSubThreadStartLatch(); // まだコネクションが晴れてないことを示す
					break;
				}
			}
		}
		if (0 < subThreadStarted.getCount()) {
			// subthreadが死んだら復活させる処理の直後, whileの次のループに入る前に抜けてしまった場合解除してあげる
			forceUnlockSubThreadConnLatch();
		}
		System.out.println("[SRP][" + soxServer + "] going to shutdown subthread");
		
		shutdownSubThread();
		System.out.println("[SRP][" + soxServer + "] END OF run()");
		logger.info(SR2LogType.RECORD_PROCESS_STOP, "record process stop", soxServer);
	}
	
	private void startSubThreadAndWaitConnectionEstablishment() {
//		System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 1");
		synchronized (subThreadOperationLock) {
//			System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 2");
			subThreadStarted = new CountDownLatch(1);
//			System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 3");
			subThread = new SubscribeThread();
//			System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 4");
			subThread.start();
//			System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 5");
			while (isRunning) {
//				System.out.println("[SRP][" + soxServer + "] waiting for connEstablished");
				CountDownLatch latch = subThread.getConnEstablished();
				try {
					if (latch.await(250, TimeUnit.MILLISECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
//			System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 6");
			subThreadStarted.countDown();
		}
//		System.out.println("[SRP][" + soxServer + "] ##### startSubThreadAndWaitConnectionEstablishment 7 finished");
	}
	
	private void renewSubThreadStartLatch() {
//		System.out.println("[SRP][" + soxServer + "] ##### renewSubThreadStartLatch");
		synchronized (subThreadOperationLock) {
			subThreadStarted = new CountDownLatch(1);
		}
//		System.out.println("[SRP][" + soxServer + "] ##### renewSubThreadStartLatch finished");
	}
	
	private void waitSubThreadConnectionEstablishment() {
//		System.out.println("[SRP][" + soxServer + "] @@@@@ ---- >>>> waitSubThreadConnectionEstablishment 1");
		synchronized (subThreadOperationLock) {
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- >>>> waitSubThreadConnectionEstablishment 2");
			ThreadUtil.countDownLatchAwait(subThreadStarted);
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- >>>> waitSubThreadConnectionEstablishment 3");
		}
//		System.out.println("[SRP][" + soxServer + "] @@@@@ ---- >>>> waitSubThreadConnectionEstablishment 4 finished");
	}
	
	private void forceUnlockSubThreadConnLatch() {
//		System.out.println("[SRP][" + soxServer + "] ##### forceUnLockSubThreadConnLatch");
		synchronized (subThreadOperationLock) {
			subThreadStarted.countDown();
		}
//		System.out.println("[SRP][" + soxServer + "] ##### forceUnLockSubThreadConnLatch finished");
	}
	
	public void subscribe(NodeIdentifier nodeId) {
		if (!isRunning) {
			System.out.println("[SRP][" + soxServer + "] is not running, going to return");
			return;
		}
		subThread.subscribe(nodeId);
	}
	
	@Override
	public void shutdownSubProcess() {
//		System.out.println("[SRP][" + soxServer + "] @@@@@ shutdown 1");
		isRunning = false;
//		System.out.println("[SRP][" + soxServer + "] @@@@@ shutdown 2");
		shutdownSubThread();
//		System.out.println("[SRP][" + soxServer + "] @@@@@ shutdown 3 finished");
	}
	
	private synchronized void shutdownSubThread() {
		if (subThread != null && subThread.isAlive()) {
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- sub shutdown 1");
			waitSubThreadConnectionEstablishment();
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- sub shutdown 2");
			subThread.stopSubThread();
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- sub shutdown 3");
			ThreadUtil.join(subThread);
//			System.out.println("[SRP][" + soxServer + "] @@@@@ ---- sub shutdown 4 finished");
		}
	}
	
	public SoxLoginInfo getSoxLoginInfo() {
		return parent.getDefaultRecorderLoginInfo(soxServer);
	}
	
	/**
	 * このServerRecordProcessが担当するサーバにおいてまだsubscribeしてないnodeをリスト
	 * @return
	 * @throws SQLException
	 */
	public List<NodeIdentifier> getNotSubscribedObservations() throws SQLException {
		Connection conn = connManager.getConnection();

		String sql = "SELECT sox_node FROM observation WHERE sox_server = ? AND is_subscribed = ? AND is_subscribe_failed = ?;";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, soxServer);
		ps.setBoolean(2, false);
		ps.setBoolean(3, false);
		ResultSet rs = ps.executeQuery();
		connManager.updateLastCommunicateTime();
		
		List<NodeIdentifier> ret = new ArrayList<>();
		while (rs.next()) {
			String node = rs.getString(1);
			ret.add(new NodeIdentifier(soxServer, node));
		}
		rs.close();
		ps.close();
		return ret;
	}

	@Override
	public PGConnectionManager getConnManager() {
		return connManager;
	}

	@Override
	public String getComponentName() {
		return "server_record_process";
	}

}
