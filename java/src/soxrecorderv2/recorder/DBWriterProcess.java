package soxrecorderv2.recorder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import jp.ac.keio.sfc.ht.sox.protocol.Data;
import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import soxrecorderv2.cache.NodeInfo;
import soxrecorderv2.common.model.LargeObjectContainer;
import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.RecordTask;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.logging.SR2LogType;
import soxrecorderv2.logging.SR2Logger;
import soxrecorderv2.util.GzipUtil;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.SQLUtil;

public class DBWriterProcess implements Runnable, RecorderSubProcess {
	
	public static final String PG_DRIVER = "org.postgresql.Driver";
	
	public static final String CONFIG_KEY_PG_HOST = "pg_host";
	public static final String CONFIG_KEY_PG_DBNAME = "pg_dbname";
	public static final String CONFIG_KEY_PG_USER = "pg_user";
	public static final String CONFIG_KEY_PG_PASS = "pg_pass";

	private static final String[] RAW_VALUE_INSERT_FIELDS = {
		"record_id",            // 1
		"has_same_typed_value", // 2
		"value_type",           // 3
		"transducer_id",        // 4
		"string_value",         // 5
		"int_value",            // 6
		"float_value",          // 7
		"decimal_value",        // 8
		"large_object_id",      // 9
		"transducer_timestamp"  // 10
	};
	public static final String[] TYPED_VALUE_INSERT_FIELDS = {
		"record_id",      // 1
		"value_type",     // 2
		"transducer_id",  // 3
		"string_value",   // 4
		"int_value",      // 5
		"float_value",    // 6
		"decimal_value",  // 7
		"large_object_id" // 8
	};
	private static final String RAW_VALUE_INSERT_SQL;
	private static final String TYPED_VALUE_INSERT_SQL;

	private static final String[] LARGE_OBJECT_INSERT_FIELDS = {
		"is_gzipped",    // 1
		"hash_key",      // 2
		"content",       // 3
		"content_length" // 4
	};
	private static final String LARGE_OBJECT_INSERT_SQL;

	private static final Cache<NodeIdentifier, NodeInfo> cache;

	static {
		RAW_VALUE_INSERT_SQL = SQLUtil.buildInsertSql(SR2Tables.TransducerRawValue, RAW_VALUE_INSERT_FIELDS);
		TYPED_VALUE_INSERT_SQL = SQLUtil.buildInsertSql(SR2Tables.TransducerTypedValue, TYPED_VALUE_INSERT_FIELDS);
		LARGE_OBJECT_INSERT_SQL = SQLUtil.buildInsertSql(SR2Tables.LargeObject, LARGE_OBJECT_INSERT_FIELDS);
		
		Cache<NodeIdentifier, NodeInfo> cacheTmp = CacheBuilder.newBuilder()
				.concurrencyLevel(1)
				.maximumSize(25000)
				.expireAfterAccess(3600, TimeUnit.SECONDS)
				.build();
		
		cache = cacheTmp;
	}
	
	@SuppressWarnings("unused")
	private Recorder parent;
	
	private LinkedBlockingQueue<RecordTask> recordTaskQueue;
	private volatile boolean isRunning;
	private PGConnectionManager connManager;
	private SR2Logger logger;
	
	public DBWriterProcess(Recorder parent, LinkedBlockingQueue<RecordTask> recordTaskQueue) {
		this.parent = parent;
		this.logger = parent.createLogger(getComponentName());
		this.recordTaskQueue = recordTaskQueue;
		this.isRunning = false;
		this.connManager = new PGConnectionManager(parent.getConfig());
		
		try {
			Class.forName(PG_DRIVER);
		} catch (ClassNotFoundException e) {
			// FIXME: it means PostgreSQL driver was not found
		}
	}
	
	@Override
	public void run() {
		logger.info(SR2LogType.DB_WRITER_START, "db writer start");
		isRunning = true;
		
		while (isRunning) {
//			System.out.println("[DBW] waiting for new task");
			RecordTask newTask = null;
			try {
				newTask = recordTaskQueue.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.error(SR2LogType.JAVA_INTERRUPTED_EXCEPTION, "during task fetching", e);
			}
			if (newTask == null) {
				continue;  // timeout
			}
//			System.out.println("[DBW] got new task");
			
			try {
				writeToDatabase(newTask);
			} catch (Exception e) {
				// Auto-generated catch block
				logger.error(SR2LogType.JAVA_GENERAL_EXCEPTION, "uncaught exception", e);
			}
//			System.out.println("[DBW] wrote! server=" + newTask.getNodeId().getServer() + ", node=" + newTask.getNodeId().getNode());
			
			// FIXME ミドルウェア(PGなど)との通信に失敗したらログにJSON形式で保存してあとで、そこから投入できるようにする
		}
		System.err.println("[DBW] END OF run()");
		connManager.close();
		logger.info(SR2LogType.DB_WRITER_STOP, "db writer stop");
	}
	
	/**
	 * データベースに保存する処理
	 * 
	 * @param task
	 * @throws SQLException
	 */
	private void writeToDatabase(RecordTask task) throws SQLException {
		Data soxData = task.getData();
		final NodeIdentifier nodeId = task.getNodeId();
		final List<TransducerValue> tValues = soxData.getTransducerValue();
		
		// トランザクションを開始する
		boolean gotProblem = false;
		Connection conn = connManager.getConnection();
		Savepoint savePointBeforeWrite = conn.setSavepoint();
		try {
			NodeInfo nodeInfo = cache.get(nodeId, new Callable<NodeInfo>() {
				@Override
				public NodeInfo call() throws Exception {
					return resolveNodeInfo(nodeId, tValues);
				}
			});
			
			if (!nodeInfo.isCovering(tValues)) {
				// キャッシュでidがわからないtransducerがあった場合
				nodeInfo = resolveNodeInfo(nodeId, tValues);
				cache.put(nodeId, nodeInfo);
			}
			
			// 1. observationのidをSQLからひいてくる, みつからなかったらトランザクションROLLBACKして終了?
			long observationId = nodeInfo.getObservationId();
			
			// 2. recordレコードを作成する。recordのidを取得
			long recordId = insertDataRecord(observationId, task, nodeId);
			
			// 3. raw_xmlレコードを作成する。
			String rawXml = task.getRawXml();
			insertRawXml(recordId, rawXml, nodeId);
			
			Collection<LargeObjectContainer> largeObjects = SOXUtil.extractLargeObjects(tValues);
			Map<String, Long> loInfo = resolveLargeObjects(largeObjects);
			insertRawValues(nodeInfo, recordId, tValues, loInfo);
			
			Collection<TransducerValue> originalTypedValues = SOXUtil.extractTypedValueDifferentFromRaw(tValues);
			if (0 < originalTypedValues.size()) {
				insertTypedValues(nodeInfo, recordId, originalTypedValues, loInfo);
			}
		} catch (Exception e) {
			e.printStackTrace();
			gotProblem = true;
			logger.error(SR2LogType.JAVA_GENERAL_EXCEPTION, "uncaught exception in writeToDatabase", e);
		} finally {
			if (gotProblem) {
				// 問題があったのでrollbackする
				System.err.println("[DBW][w] going to rollback");
				conn.rollback(savePointBeforeWrite);
				System.err.println("[DBW][w] something bad happened! rollback");
			} else {
				// トランザクションをcommitする
				conn.commit();
			}
		}
	}
	
	private NodeInfo resolveNodeInfo(NodeIdentifier nodeId, Collection<TransducerValue> tValues) {
		NodeInfo ret = null;
		while (ret == null ) {
			try {
				ret = _resolveNodeInfo(nodeId, tValues);
			} catch (SQLException e) {
				logger.error(SR2LogType.JAVA_SQL_EXCEPTION, "SQL exception during resolveNodeInfo", e);
			}
		}
		return ret;
	}
	
	private NodeInfo _resolveNodeInfo(NodeIdentifier nodeId, Collection<TransducerValue> tValues) throws SQLException {
		final long observationId = resolveObservationId(nodeId);
		final Map<String, Long> transducerIdMap = resolveTransducers(observationId, SOXUtil.extractTransducerIds(tValues), nodeId);
		return new NodeInfo(nodeId, observationId, transducerIdMap);
	}
	
	private Map<String, Long> resolveLargeObjects(Collection<LargeObjectContainer> largeObjects) throws SQLException {
		if (largeObjects.isEmpty()) {
			return new HashMap<>();
		}
		Connection conn = connManager.getConnection();
		
		// まず, DBに存在しないか確認する
		Collection<LargeObjectContainer> uniqueLargeObjects = SOXUtil.uniqueLargeObjects(largeObjects);
		Collection<String> hashes = SOXUtil.extractLargeObjectHashes(uniqueLargeObjects);  // 重複を排除
		String sqlCheck = "SELECT hash_key, id FROM large_object WHERE hash_key IN " + SQLUtil.buildPlaceholders(hashes.size()) + ";";
		PreparedStatement psCheck = conn.prepareStatement(sqlCheck);
		int idx = 1;
		for (String hash : hashes) {
			psCheck.setString(idx++, hash);
		}
		ResultSet rsCheck = psCheck.executeQuery();
		connManager.updateLastCommunicateTime();
		
		Map<String, Long> loIdMap = new HashMap<>();
		while (rsCheck.next()) {
			String hash = rsCheck.getString(1);
			long loId = rsCheck.getLong(2);
			loIdMap.put(hash, loId);
		}
		rsCheck.close();
		psCheck.close();
		
		// 全部ある
		if (loIdMap.size() == uniqueLargeObjects.size()) {
			return Collections.unmodifiableMap(loIdMap);
		}
		
		// ないものを追加する
		PreparedStatement psInsert = conn.prepareStatement(LARGE_OBJECT_INSERT_SQL);
		List<String> missedLargeObjectHashes = new ArrayList<>();
		for (LargeObjectContainer loContainer : uniqueLargeObjects) {
			String hash = loContainer.getHash();
			if (!loIdMap.containsKey(hash)) {
				fillLargeObjectValues(psInsert, loContainer);
				psInsert.addBatch();
				missedLargeObjectHashes.add(loContainer.getHash());
			}
		}
		psInsert.executeBatch();
		connManager.updateLastCommunicateTime();
		psInsert.close();
		
		// 問い合わせなおす
		sqlCheck = "SELECT hash_key, id FROM large_object WHERE hash_key IN " + SQLUtil.buildPlaceholders(missedLargeObjectHashes.size()) + ";";
		psCheck = conn.prepareStatement(sqlCheck);
		idx = 1;
		for (String hash : missedLargeObjectHashes) {
			psCheck.setString(idx++, hash);
		}
		rsCheck = psCheck.executeQuery();
		connManager.updateLastCommunicateTime();
		
		while (rsCheck.next()) {
			String hash = rsCheck.getString(1);
			long loId = rsCheck.getLong(2);
			loIdMap.put(hash, loId);
		}
		
		psCheck.close();
		rsCheck.close();
		
		return Collections.unmodifiableMap(loIdMap);
	}
	
	private void fillLargeObjectValues(PreparedStatement ps, LargeObjectContainer largeObject) throws SQLException {
		String hexHash = largeObject.getHash();
		byte[] largeObjectContent = largeObject.getData();
		
		// gzipしてみる
		int contentLength = largeObjectContent.length;
		byte[] sqlArgContent;
		boolean isGzipped;
		
		try {
			byte[] compressedContent = GzipUtil.compress(largeObjectContent);
			if (largeObjectContent.length < compressedContent.length) {
				// gzipしたらおおきくなっちゃった: use orignal
				isGzipped = false;
				sqlArgContent = largeObjectContent;
			} else {
				isGzipped = true;
				sqlArgContent  = compressedContent;
			}
		} catch (IOException e) {
			sqlArgContent = largeObjectContent;
			isGzipped = false;
		}
		
		ps.setBoolean(1, isGzipped);
		ps.setString(2, hexHash);
		ps.setBytes(3, sqlArgContent);
		ps.setLong(4, contentLength);
	}
	
	private void insertRawValues(NodeInfo nodeInfo, long recordId, Collection<TransducerValue> tValues, Map<String, Long> loInfo) throws SQLException, ParseException {
		Connection conn = connManager.getConnection();
		PreparedStatement ps = conn.prepareStatement(RAW_VALUE_INSERT_SQL);
		
		for (TransducerValue value : tValues) {
			if (value == null) {
				continue;
			}
			fillRawInsertPreparedStatement(nodeInfo, recordId, ps, value, loInfo);
			ps.addBatch();
		}
		
		ps.executeBatch();
		connManager.updateLastCommunicateTime();
		ps.close();
	}
	
	private void insertTypedValues(NodeInfo nodeInfo, long recordId, Collection<TransducerValue> tValues, Map<String, Long> loInfo) throws SQLException {
		Connection conn = connManager.getConnection();
		PreparedStatement ps = conn.prepareStatement(TYPED_VALUE_INSERT_SQL);
		
		for (TransducerValue value : tValues) {
			if (value == null) {
				continue;
			}
			fillTypeInsertPreparedStatement(nodeInfo, recordId, ps, value, loInfo);
			ps.addBatch();
		}
		ps.executeBatch();
		connManager.updateLastCommunicateTime();
		ps.close();
	}
	
	private void fillRawInsertPreparedStatement(NodeInfo nodeInfo, long recordId, PreparedStatement ps, TransducerValue value, Map<String, Long> loInfo) throws SQLException, ParseException {
		String tdrIdentity = value.getId();
//		long tdrRecordId = nodeInfo.getTransducerIdMap().get(tdrIdentity);
		Map<String, Long> tdrIdMap = nodeInfo.getTransducerIdMap();
		if (!tdrIdMap.containsKey(tdrIdentity)) {
			// TODO: おこりえないはずだが
		}
		long tdrRecordId = tdrIdMap.get(tdrIdentity);
		String rawValue = value.getRawValue();
		int valType = SOXUtil.guessValueType(tdrIdentity, rawValue);
		boolean hasSameTypedValue = SOXUtil.hasSameTypedValue(value);

		ps.setLong(1, recordId);
		ps.setBoolean(2, hasSameTypedValue);
		ps.setInt(3, valType);
		ps.setLong(4, tdrRecordId);
		
		if (valType == SOXUtil.VALUE_TYPE_STRING) {
			ps.setString(5, rawValue);
		} else {
			ps.setNull(5, Types.VARCHAR);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_INT) {
			ps.setInt(6, Integer.parseInt(rawValue));
		} else {
			ps.setInt(6, 0);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_FLOAT || valType == SOXUtil.VALUE_TYPE_DECIMAL) {
			// float_value
			ps.setDouble(7, Double.parseDouble(rawValue));
		} else {
			ps.setDouble(7, 0.0);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_DECIMAL || valType == SOXUtil.VALUE_TYPE_FLOAT) {
			// decimal_value
			ps.setBigDecimal(8, new BigDecimal(rawValue));
		} else {
			ps.setBigDecimal(8, new BigDecimal("0.0"));
		}
		
		if (valType == SOXUtil.VALUE_TYPE_LARGE_OBJECT) {
			LargeObjectContainer loContainer = new LargeObjectContainer(value, true);
			String loHash = loContainer.getHash();
			if (!loInfo.containsKey(loHash)) {
				// TODO: おこりえないはずだが
			}
			long largeObjectId = loInfo.get(loHash);
			ps.setLong(9, largeObjectId);
		} else {
			ps.setNull(9, Types.INTEGER);
		}
		
		Timestamp tdrTimestamp = SOXUtil.parseTransducerTimeStamp(value);
		ps.setTimestamp(10, tdrTimestamp);
	}
	
	private void fillTypeInsertPreparedStatement(NodeInfo nodeInfo, long recordId, PreparedStatement ps, TransducerValue value, Map<String, Long> loInfo) throws SQLException {
		String tdrIdentity = value.getId();
		Map<String, Long> tdrIdMap = nodeInfo.getTransducerIdMap();
		if (!tdrIdMap.containsKey(tdrIdentity)) {
			// TODO ありえないはずだが
		}
		long transducerRecordId = tdrIdMap.get(tdrIdentity);
		ps.setLong(1, recordId);

		String rawValue = value.getRawValue();
		
		int valType = SOXUtil.guessValueType(tdrIdentity, rawValue);
		
		ps.setInt(2, valType);
		
		ps.setLong(3, transducerRecordId);
		
		if (valType == SOXUtil.VALUE_TYPE_STRING) {
			ps.setString(4, rawValue);
		} else {
			ps.setNull(4, Types.VARCHAR);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_INT) {
			ps.setInt(5, Integer.parseInt(rawValue));
		} else {
			ps.setInt(5, 0);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_FLOAT || valType == SOXUtil.VALUE_TYPE_DECIMAL) {
			ps.setDouble(6, Double.parseDouble(rawValue));
		} else {
			ps.setDouble(6, 0.0);
		}
		
		if (valType == SOXUtil.VALUE_TYPE_DECIMAL || valType == SOXUtil.VALUE_TYPE_FLOAT) {
			ps.setBigDecimal(7, new BigDecimal(rawValue));
		} else {
			ps.setBigDecimal(7, new BigDecimal("0.0"));
		}

		if (valType == SOXUtil.VALUE_TYPE_LARGE_OBJECT) {
			LargeObjectContainer loContainer = new LargeObjectContainer(value, false);
			String loHash = loContainer.getHash();
			if (!loInfo.containsKey(loHash)) {
				// TODO: おこりえないはずだが
			}
			long largeObjectId = loInfo.get(loHash);
			ps.setLong(8, largeObjectId);
		} else {
			ps.setNull(8, Types.INTEGER);
		}
	}
	
	/**
	 * "record"テーブルにレコードをinsertする
	 * @param recordTask
	 * @return
	 * @throws SQLException 
	 */
	private long insertDataRecord(long observationId, RecordTask recordTask, NodeIdentifier nodeId) throws SQLException {
		Connection conn = connManager.getConnection();
		
		// このやりかたはいいのか？ http://alvinalexander.com/java/java-timestamp-example-current-time-now
//		Calendar calendar = Calendar.getInstance();
//		java.util.Date nowDate = calendar.getTime();
//		java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(nowDate.getTime());
		java.sql.Timestamp currentTimestamp = SQLUtil.getCurrentTimestamp();
		
		String[] fields = { "observation_id", "is_parse_error", "created" };
		String sql = SQLUtil.buildInsertSql(SR2Tables.Record, fields);
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, observationId);
		ps.setBoolean(2, false); // FIXME
		ps.setTimestamp(3, currentTimestamp);
		
		int affectedRows = ps.executeUpdate();
		if (affectedRows != 1) {
			System.err.println("[DBW][w][2] could not insert to record table");
			logger.error(SR2LogType.RECORD_FAILED, "record failed", nodeId.getServer(), nodeId.getNode());
		} else {
			logger.debug(SR2LogType.RECORD, "record", nodeId.getServer(), nodeId.getNode());
		}
		ps.close();
		
		// 問い合わせる, ユニークなキーがないので lastval() の値をひく。
		final String sqlGetLastVal = "SELECT lastval();";
		final PreparedStatement psGetLastVal = conn.prepareStatement(sqlGetLastVal);
		final ResultSet rsGetLastVal = psGetLastVal.executeQuery();
		connManager.updateLastCommunicateTime();
		if (!rsGetLastVal.next()) {
			System.err.println("[DBW][w][2] could not retrieve lastval() query call");
		}
		long insertedRecordId = rsGetLastVal.getLong(1);
//		System.out.println("[DBW][w][2] record insert success, insertedRecordId=" + insertedRecordId);
		rsGetLastVal.close();
		psGetLastVal.close();
		return insertedRecordId;
	}
	
	/**
	 * raw_xmlテーブルにレコードをinsertする。とくにリレーションで必要というわけではないのでidは引き直さない
	 * @param recordId
	 * @param rawXml
	 * @throws IOException 
	 * @throws SQLException 
	 */
	private void insertRawXml(long recordId, String rawXml, NodeIdentifier nodeId) throws IOException, SQLException {
		String[] fields = { "record_id", "is_gzipped", "raw_xml" };
		
		boolean isGzipped = true;  // we know compressed data must be smaller than raw xml
		byte[] bytesRawXml = rawXml.getBytes("UTF-8");
		byte[] compressed = GzipUtil.compress(bytesRawXml);
		if (compressed.length == 0) {
			logger.warn(SR2LogType.RAW_XML_INSERT_FAILED, "compressed.length=0", nodeId.getServer(), nodeId.getNode());
		}
		ByteArrayInputStream contentStream = new ByteArrayInputStream(compressed);
		
		Connection conn = connManager.getConnection();
		final String sql = SQLUtil.buildInsertSql(SR2Tables.RawXml, fields);
		final PreparedStatement ps = conn.prepareStatement(sql);
		
		ps.setLong(1, recordId);
		ps.setBoolean(2, isGzipped);
		ps.setBinaryStream(3, contentStream);
		
		int affectedRows = ps.executeUpdate();
		connManager.updateLastCommunicateTime();
		if (affectedRows != 1) {
//			System.out.println("[DBW][w][3] insert to raw_xml failed...");
			logger.error(SR2LogType.RAW_XML_INSERT_FAILED, "raw xml insert failed, " + ps.getWarnings(), nodeId.getServer(), nodeId.getNode());
		} else {
			logger.debug(SR2LogType.RAW_XML_INSERT, "raw xml inserted", nodeId.getServer(), nodeId.getNode());
		}
		
		ps.close();
	}
	
	/**
	 * トランザクションの中と仮定してよい
	 * @param observationId
	 * @param transducerIds
	 * @param nodeId
	 * @return
	 * @throws SQLException
	 */
	private Map<String, Long> resolveTransducers(long observationId, Collection<String> transducerIds, NodeIdentifier nodeId) throws SQLException {
		final Connection conn = connManager.getConnection();
		
		String sql = "SELECT id, transducer_id FROM transducer WHERE observation_id = ?;";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, observationId);
		ResultSet rs = ps.executeQuery();
		connManager.updateLastCommunicateTime();
		Map<String, Long> tid2dbid = new HashMap<>();
		while (rs.next()) {
			long tdrDatabaseId = rs.getLong(1);
			String transducerId = rs.getString(2);
			tid2dbid.put(transducerId, tdrDatabaseId);
		}
		rs.close();
		ps.close();
		
		Set<String> missingTransducers = new HashSet<>();
		for (String tid : transducerIds) {
			if (!tid2dbid.containsKey(tid)) {
				missingTransducers.add(tid);
			}
		}
		
		if (missingTransducers.isEmpty()) {
			return Collections.unmodifiableMap(tid2dbid);
		}
		
		String insertSql = "INSERT INTO transducer(observation_id, transducer_id) VALUES (?, ?);";
		PreparedStatement insertPs = conn.prepareStatement(insertSql);
		for (String missingTdr : missingTransducers) {
			insertPs.setLong(1, observationId);
			insertPs.setString(2, missingTdr);
			insertPs.addBatch();
		}
		insertPs.executeBatch();
		connManager.updateLastCommunicateTime();
		insertPs.close();
		
		String lookupSql = "SELECT id, transducer_id FROM transducer WHERE observation_id = ? AND transducer_id IN " + SQLUtil.buildPlaceholders(missingTransducers.size()) + ";";
		PreparedStatement lookupPs = conn.prepareStatement(lookupSql);
		lookupPs.setLong(1, observationId);
		int idx = 2;
		for (String missingTdr : missingTransducers) {
			lookupPs.setString(idx++, missingTdr);
		}
		ResultSet lookupRs = lookupPs.executeQuery();
		connManager.updateLastCommunicateTime();
		while (lookupRs.next()) {
			long tdrDatabaseId = lookupRs.getLong(1);
			String transducerId = lookupRs.getString(2);
			tid2dbid.put(transducerId, tdrDatabaseId);
		}
		lookupRs.close();
		lookupPs.close();
		
		return Collections.unmodifiableMap(tid2dbid);
	}
	
	/**
	 * トランザクションの中だと仮定してよい
	 * @param nodeIdentifier
	 * @return
	 * @throws SQLException 
	 */
	private long resolveObservationId(NodeIdentifier nodeIdentifier) throws SQLException {
		String soxServer = nodeIdentifier.getServer();
		String soxNode = nodeIdentifier.getNode();
		
		// SQLでひく
		Connection conn = connManager.getConnection();
		final String sql = "SELECT id FROM observation WHERE sox_server = ? AND sox_node = ?;";
		final PreparedStatement ps = conn.prepareStatement(sql);
		connManager.updateLastCommunicateTime();
		ps.setString(1, soxServer);
		ps.setString(2, soxNode);
		ResultSet rs = ps.executeQuery();
		connManager.updateLastCommunicateTime();
		if (!rs.next()) {
			// FIXME: みつからなかった
//			System.out.println("[DBW][w][1] observation for server=" + soxServer + ", node=" + soxNode + " was not found!");
			rs.close();
			ps.close();
			return 0;
		} else {
			long foundObservationId = rs.getLong(1);
//			System.out.println("[DBW][w][1] observation for server=" + soxServer + ", node=" + soxNode + " was found, id=" + foundObservationId);
			rs.close();
			ps.close();
			return foundObservationId;
		}
	}
	
	@Override
	public void shutdownSubProcess() {
		System.out.println("[DBW] @@@@@ shutdown 1");
		isRunning = false;
		System.out.println("[DBW] @@@@@ shutdown 2 finished");
	}

	@Override
	public PGConnectionManager getConnManager() {
		return connManager;
	}

	@Override
	public String getComponentName() {
		return "dbwriter";
	}

}
