package soxrecorderv2.recorder;

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
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import jp.ac.keio.sfc.ht.sox.protocol.Data;
import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.RecordTask;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.util.GzipUtil;
import soxrecorderv2.util.HashUtil;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SOXUtil;
import soxrecorderv2.util.SQLUtil;

public class DBWriterProcess implements Runnable, RecorderSubProcess {
	
	public static final String PG_DRIVER = "org.postgresql.Driver";
	
	public static final String CONFIG_KEY_PG_HOST = "pg_host";
	public static final String CONFIG_KEY_PG_DBNAME = "pg_dbname";
	public static final String CONFIG_KEY_PG_USER = "pg_user";
	public static final String CONFIG_KEY_PG_PASS = "pg_pass";
	
	public static final int VALUE_TYPE_STRING       = 1;
	public static final int VALUE_TYPE_INT          = 2;
	public static final int VALUE_TYPE_FLOAT        = 3;
	public static final int VALUE_TYPE_DECIMAL      = 4;
	public static final int VALUE_TYPE_LARGE_OBJECT = 5;
	
	public static final Pattern PATTERN_INT;
	public static final Pattern PATTERN_FLOAT;
	
	public static final String[] DECIMAL_NAMES = {
		"lat",
		"lng",
		"latitude",
		"longitude"
	};

	static {
		Pattern patInt = null;
		Pattern patFloat = null;
		
		patInt = Pattern.compile("\\A-?[0-9]+\\z");
		patFloat = Pattern.compile("\\A-?[0-9]+\\.[0-9]+\\z");
		
		PATTERN_INT = patInt;
		PATTERN_FLOAT = patFloat;
	}
	
	@SuppressWarnings("unused")
	private Recorder parent;
	
	private LinkedBlockingQueue<RecordTask> recordTaskQueue;
	private boolean isRunning;
	private PGConnectionManager connManager;
	
	public DBWriterProcess(Recorder parent, LinkedBlockingQueue<RecordTask> recordTaskQueue) {
		this.parent = parent;
		this.recordTaskQueue = recordTaskQueue;
		this.isRunning = false;
		this.connManager = new PGConnectionManager(parent.getConfig());
		
		try {
			Class.forName(PG_DRIVER);
		} catch (ClassNotFoundException e) {
			// FIXME: it means PostgreSQL driver was not found
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		isRunning = true;
		
		while (isRunning) {
//			System.out.println("[DBW] waiting for new task");
			RecordTask newTask = null;
			try {
				newTask = recordTaskQueue.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
			if (newTask == null) {
				continue;  // timeout
			}
//			System.out.println("[DBW] got new task");
			
			try {
				writeToDatabase(newTask);
			} catch (Exception e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("[DBW] wrote! server=" + newTask.getNodeId().getServer() + ", node=" + newTask.getNodeId().getNode());
			
			// FIXME ミドルウェア(PGなど)との通信に失敗したらログにJSON形式で保存してあとで、そこから投入できるようにする
		}
		System.err.println("[DBW] END OF run()");
	}
	
	/**
	 * データベースに保存する処理
	 * 
	 * @param task
	 * @throws SQLException
	 */
	private void writeToDatabase(RecordTask task) throws SQLException {
		NodeIdentifier nodeId = task.getNodeId();
		Data soxData = task.getData();
		
		// トランザクションを開始する
		boolean gotProblem = false;
		Connection conn = connManager.getConnection();
		Savepoint savePointBeforeWrite = conn.setSavepoint();
//		System.out.println("[DBW][w] 0.1 got connection");
//		conn.setAutoCommit(false);  // start transaction
//		System.out.println("[DBW][w] 0.2 started transaction");
		try {
			// 1. observationのidをSQLからひいてくる, みつからなかったらトランザクションROLLBACKして終了?
			long observationId = resolveObservationId(nodeId);
			if (observationId == 0) {
				// 解決に失敗している
				throw new RuntimeException("no such observation");
			}
//			System.out.println("[DBW][w] 1. obid resolved, obid=" + observationId);
			
			// 2. recordレコードを作成する。recordのidを取得
			long recordId = insertDataRecord(observationId, task);
//			System.out.println("[DBW][w] 2. record inserted");
			
			// 3. raw_xmlレコードを作成する。
			String rawXml = task.getRawXml();
			insertRawXml(recordId, rawXml);
//			System.out.println("[DBW][w] 3. raw xml inserted");
			
			// 各transducerの値についてrelationさせながらいれていく
			List<TransducerValue> tValues = soxData.getTransducerValue();
			for (TransducerValue tv : tValues) {
				String tId = tv.getId();
				
				// 4. transducer_idを解決する。必要に応じて, 新規のtransducerレコードを作成してidを得る
				long dbTdrId = resolveTranducerDatabaseId(observationId, tId);
//				System.out.println("[DBW][w][4][" + tId + "] resolved db id: dbTdrId=" + dbTdrId);

				// transducer_raw_valueレコードをつくる
				boolean hasSameTypedValue = hasSameTypedValue(tv);
				boolean isRawRecordSuccess = insertTransducerRawValue(observationId, recordId, dbTdrId, tv, hasSameTypedValue);
				if (!isRawRecordSuccess) {
					// FIXME なにかがおかしい
					System.err.println("[DBW][w][4][" + tId + "] failed to insert to transducer_raw_value");
				}

				// transducer_typed_valueレコードをつくる(rawとおなじならrawのhas_same_typed_valueをtrueにして作らなくてよい。)
				if (!hasSameTypedValue) {
					boolean isTypedRecordSuccess = insertTransducerTypedValue(recordId, dbTdrId, tv);
					if (!isTypedRecordSuccess) {
						// FIXME なにかがおかしい
						System.err.println("[DBW][w][4][" + tId + "] failed to insert to transducer_typed_value");
					}
				} else {
//					System.out.println("[DBW][w][4][" + tId + "] no need to insert transducer_typed_value");
				}
			}
//			System.out.println("[DBW][w][4] finished inserting all transducer values");
		} catch (Exception e) {
			gotProblem = true;
			e.printStackTrace(); // FIXME
		} finally {
			if (gotProblem) {
				// 問題があったのでrollbackする
				System.err.println("[DBW][w] going to rollback");
				conn.rollback(savePointBeforeWrite);
				System.err.println("[DBW][w] something bad happened! rollback");
			} else {
				// トランザクションをcommitする
//				System.out.println("[DBW][w] going to commit transaction");
				conn.commit();
//				System.out.println("[DBW][w] transaction commited");
			}
		}
	}
	
	private boolean insertTransducerRawValue(
			long observationId,
			long recordId, long transducerRecordId, TransducerValue value,
			boolean hasSameTypedValue) throws SQLException, IOException, ParseException {
		Connection conn = connManager.getConnection();
		
		String[] fields = {
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
		
		String tdrIdentity = value.getId();
		String rawValue = value.getRawValue();
		
		int valType = guessValueType(tdrIdentity, rawValue);
		
		long tdrRecordId = resolveTranducerDatabaseId(observationId, tdrIdentity);
		
		String sql = SQLUtil.buildInsertSql(SR2Tables.TransducerRawValue, fields);
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, recordId);
		ps.setBoolean(2, hasSameTypedValue);
		ps.setInt(3, valType);
		ps.setLong(4, tdrRecordId);
		
		if (valType == VALUE_TYPE_STRING) {
			ps.setString(5, rawValue);
		} else {
			ps.setNull(5, Types.VARCHAR);
		}
		
		if (valType == VALUE_TYPE_INT) {
			ps.setInt(6, Integer.parseInt(rawValue));
		} else {
			ps.setInt(6, 0);  // FIXME is this ok?
		}
		
		if (valType == VALUE_TYPE_FLOAT || valType == VALUE_TYPE_DECIMAL) {
			// float_value
			ps.setDouble(7, Double.parseDouble(rawValue));
		} else {
			ps.setDouble(7, 0.0); // FIXME is this ok?
		}
		
		if (valType == VALUE_TYPE_DECIMAL || valType == VALUE_TYPE_FLOAT) {
			// decimal_value
			ps.setBigDecimal(8, new BigDecimal(rawValue));
		} else {
			ps.setBigDecimal(8, new BigDecimal("0.0"));
		}
		
		if (valType == VALUE_TYPE_LARGE_OBJECT) {
			byte[] largeObject = null;
			largeObject = rawValue.getBytes("UTF-8");
			long largeObjectId = findOrPutLargeObject(largeObject);
			ps.setLong(9, largeObjectId);
		} else {
//			ps.setLong(9, 0);  // FIXME is this ok?
			ps.setNull(9, Types.INTEGER);
		}
		
		Timestamp tdrTimestamp = SOXUtil.parseTransducerTimeStamp(value);
		ps.setTimestamp(10, tdrTimestamp);
		
		int affectedRows = ps.executeUpdate();
		connManager.updateLastCommunicateTime();
		boolean result;
		if (affectedRows != 1) {
			// FIXME: something wrong happened
			System.err.println("[DBW][w][4][raw][" + tdrIdentity + "] could not insert to transducer_raw_value");
			result = false;
		} else {
			result = true;
		}
		
		ps.close();
		return result;
	}
	
	private boolean insertTransducerTypedValue(
			long recordId, long transducerRecordId, TransducerValue value) throws SQLException, IOException {
		String[] fields = {
			"record_id",      // 1
			"value_type",     // 2
			"transducer_id",  // 3
			"string_value",   // 4
			"int_value",      // 5
			"float_value",    // 6
			"decimal_value",  // 7
			"large_object_id" // 8
		};
		
		Connection conn = connManager.getConnection();
		String sql = SQLUtil.buildInsertSql(SR2Tables.TransducerTypedValue, fields);
		PreparedStatement ps = conn.prepareStatement(sql);
		
		ps.setLong(1, recordId);

		String tdrIdentity = value.getId();
		String rawValue = value.getRawValue();
		
		int valType = guessValueType(tdrIdentity, rawValue);
		
		ps.setInt(2, valType);
		
		ps.setLong(3, transducerRecordId);
		
		if (valType == VALUE_TYPE_STRING) {
			ps.setString(4, rawValue);
		} else {
			ps.setNull(4, Types.VARCHAR);
		}
		
		if (valType == VALUE_TYPE_INT) {
			ps.setInt(5, Integer.parseInt(rawValue));
		} else {
			ps.setInt(5, 0);  // FIXME is this ok?
		}
		
		if (valType == VALUE_TYPE_FLOAT || valType == VALUE_TYPE_DECIMAL) {
			ps.setDouble(6, Double.parseDouble(rawValue));
		} else {
			ps.setDouble(6, 0.0);
		}
		
		if (valType == VALUE_TYPE_DECIMAL || valType == VALUE_TYPE_FLOAT) {
			ps.setBigDecimal(7, new BigDecimal(rawValue));
		} else {
			ps.setBigDecimal(7, new BigDecimal("0.0"));
		}

		if (valType == VALUE_TYPE_LARGE_OBJECT) {
			byte[] largeObject = null;
			largeObject = rawValue.getBytes("UTF-8");
			long largeObjectId = findOrPutLargeObject(largeObject);
			ps.setLong(8, largeObjectId);
		} else {
//			ps.setLong(8, 0);  // FIXME is this ok?
			ps.setNull(8, Types.INTEGER);
		}
		
		int affectedRows = ps.executeUpdate();
		connManager.updateLastCommunicateTime();
		boolean result;
		if (affectedRows != 1) {
			// FIXME something wrong happened
			System.err.println("[DBW][w][4][typed][" + tdrIdentity + "] could not insert to transducer_typed_value");
			result = false;
		} else {
			result = true;
		}
		ps.close();
		return result;
	}
	
	private boolean isDecimalName(String transducerId) {
		String lowerId = transducerId.toLowerCase();
		for (String decimalId : DECIMAL_NAMES) {
			if (decimalId.equals(lowerId)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * valueの形式からtypeを推測する。
	 * 場合によってはtransducerIdも推測に必要なので値としてとる。
	 * (transducerIdがlatやlngだったら緯度経度なので精度誤差を出さないためにVALUE_TYPE_DECIMALにするなど
	 * 
	 * @param transducerId
	 * @param value
	 * @return
	 */
	private int guessValueType(String transducerId, String value) {
		if (255 < value.length()) {
			return VALUE_TYPE_LARGE_OBJECT;
		} else if (PATTERN_INT.matcher(value).matches()) {
			return VALUE_TYPE_INT;
		} else if (PATTERN_FLOAT.matcher(value).matches()) {
			if (isDecimalName(transducerId)) {
				return VALUE_TYPE_DECIMAL;
			} else {
				return VALUE_TYPE_FLOAT;
			}
		} else {
			return VALUE_TYPE_STRING;
		}
	}
	
	/**
	 * "record"テーブルにレコードをinsertする
	 * @param recordTask
	 * @return
	 * @throws SQLException 
	 */
	private long insertDataRecord(long observationId, RecordTask recordTask) throws SQLException {
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
			// FIXME なにかがおかしい
			System.err.println("[DBW][w][2] could not insert to record table");
		}
		ps.close();
		
		// 問い合わせる, ユニークなキーがないので lastval() の値をひく。
		final String sqlGetLastVal = "SELECT lastval();";
		final PreparedStatement psGetLastVal = conn.prepareStatement(sqlGetLastVal);
		final ResultSet rsGetLastVal = psGetLastVal.executeQuery();
		connManager.updateLastCommunicateTime();
		if (!rsGetLastVal.next()) {
			// FIXME なにかがおかしい
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
	private void insertRawXml(long recordId, String rawXml) throws IOException, SQLException {
		String[] fields = { "record_id", "is_gzipped", "raw_xml" };
		
		boolean isGzipped = true;  // we know compressed data must be smaller than raw xml
		byte[] bytesRawXml = rawXml.getBytes("UTF-8");
		byte[] compressed = GzipUtil.compress(bytesRawXml);
		
		Connection conn = connManager.getConnection();
		final String sql = SQLUtil.buildInsertSql(SR2Tables.RawXml, fields);
		final PreparedStatement ps = conn.prepareStatement(sql);
		
		ps.setLong(1, recordId);
		ps.setBoolean(2, isGzipped);
		ps.setBytes(3, compressed);
		
		int affectedRows = ps.executeUpdate();
		connManager.updateLastCommunicateTime();
		if (affectedRows != 0) {
			// FIXME なにかがおかしい
//			System.out.println("[DBW][w][3] insert to raw_xml failed...");
		}
		
		ps.close();
	}
	
	/**
	 * トランザクションの中だと仮定してよい
	 * @param largeObjectContent
	 * @return
	 * @throws SQLException 
	 * @throws IOException 
	 */
	private long findOrPutLargeObject(byte[] largeObjectContent) throws SQLException, IOException {
		// 1. hash値を計算して、SQLでひいてみる
		String hexHash = HashUtil.sha256(largeObjectContent);
		
		// 2. 存在してればidをかえす OR 存在しなければSQLにつっこんでidをかえす
		Connection conn = connManager.getConnection();
		final String sql = "SELECT id FROM large_object WHERE hash_key = ?;";
		final PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, hexHash);
//		System.out.println("[DBW][w][4][large_object] going to query large_object by hash=" + hexHash);
		ResultSet rs = ps.executeQuery();
		connManager.updateLastCommunicateTime();
		try {
			if (!rs.next()) {
				// ない => large_object レコードを作成
//				System.out.println("[DBW][w][4][large_object] not found, going to create large_object, hash=" + hexHash);
				
				// gzipしてみる
				byte[] compressedContent = GzipUtil.compress(largeObjectContent);
				
				int contentLength = largeObjectContent.length;
				byte[] sqlArgContent;
				boolean isGzipped;
				if (largeObjectContent.length < compressedContent.length) {
					// gzipしたらおおきくなっちゃった: use orignal
					isGzipped = false;
					sqlArgContent = largeObjectContent;
				} else {
					isGzipped = true;
					sqlArgContent  = compressedContent;
				}
				
				// insert to PostgreSQL
//				final String sqlPut = "INSERT INTO large_object(is_gzipped, hash_key, content, content_length) VALUES (?, ?, ?, ?);";
				String[] fields = {
					"is_gzipped",
					"hash_key",
					"content",
					"content_length"
				};
				final String sqlPut = SQLUtil.buildInsertSql(SR2Tables.LargeObject, fields);
				final PreparedStatement psPut = conn.prepareStatement(sqlPut);
				psPut.setBoolean(1, isGzipped);
				psPut.setString(2, hexHash);
				psPut.setBytes(3, sqlArgContent);;
				psPut.setLong(4, contentLength);
				int affectedRows = psPut.executeUpdate();
				connManager.updateLastCommunicateTime();
				if (affectedRows != 0) {
					// FIXME: なにかがおかしい...
					System.err.println("[DBW][w][4][large_object] failed to insert large_object");
				}
				psPut.close();
				
				// DBに問い合わせ直してidをしらべる(sqlが再利用できる)
				final PreparedStatement psAgain = conn.prepareStatement(sql);
				psAgain.setString(1, hexHash);
				ResultSet rsAgain = psAgain.executeQuery();
				connManager.updateLastCommunicateTime();
				if (!rsAgain.next()) {
					// FIXME: なにかがおかしい
					System.err.println("[DBW][w][4][large_object] failed to fetch inserted large_object by hash=" + hexHash);
				}
				long addedLargeObjectId = rsAgain.getLong(1);
				
				rsAgain.close();
				psAgain.close();
				
				return addedLargeObjectId;
			} else {
				long foundLargeObjectId = rs.getLong(1);
//				System.out.println("[DBW][w][4][large_object] found large_object for hash=" + hexHash + ", id=" + foundLargeObjectId);
				return foundLargeObjectId;
			}
		} finally {
			rs.close();
			ps.close();
		}
	}
	
	/**
	 * トランザクションの中だと仮定してよい
	 * @param observationId
	 * @param transducerId
	 * @return
	 */
	private long resolveTranducerDatabaseId(Long observationId, String transducerId) throws SQLException {
		// 1. SQLでひいてみる
		final Connection conn = connManager.getConnection();
		final String sql =  "SELECT id FROM transducer WHERE observation_id = ? AND transducer_id = ?;";
		final PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, observationId);
		ps.setString(2, transducerId);
//		System.out.println("[DBW][w][4][tdr] going to query transducer.id for transducer_id=" + transducerId);
		final ResultSet rs = ps.executeQuery();
		connManager.updateLastCommunicateTime();
		if (!rs.next()) {
			// なかった: 追加する
			System.out.println("[DBW][w][4][tdr] missing transducer_id=" + transducerId + ", going to create");
			final String[] insertFields = { "observation_id", "transducer_id" };
			final String sqlAdd = SQLUtil.buildInsertSql(SR2Tables.Transducer, insertFields);
			final PreparedStatement psAdd = conn.prepareStatement(sqlAdd);
			psAdd.setLong(1, observationId);
			psAdd.setString(2, transducerId);
			int affectedRows = psAdd.executeUpdate();
			connManager.updateLastCommunicateTime();
			if (affectedRows != 1) {
				// FIXME なにかがおかしい...
				System.err.println("[DBW][w][4][tdr] could not insert! transducer_id=" + transducerId);
			}
			psAdd.close();
			
			// ひきなおしてIDを取得する
			final PreparedStatement psAgain = conn.prepareStatement(sql);
			psAgain.setLong(1, observationId);
			psAgain.setString(2, transducerId);
			final ResultSet rsAgain = psAgain.executeQuery();
			connManager.updateLastCommunicateTime();
			if (!rsAgain.next()) {
				// FIXME: なにかがおかしい
				System.err.println("[DBW][w][4][tdr] could not retrieve result for inserted row! transducer_id=" + transducerId);
			}
			long addedTransducerId = rsAgain.getLong(1);
			rsAgain.close();
			psAgain.close();
			return addedTransducerId;
		} else {
			long foundTransducerId = rs.getLong(1);
//			System.out.println("[DBW][w][4][tdr] found transducer_id=" + transducerId + ", id=" + foundTransducerId);
			rs.close();
			ps.close();
			return foundTransducerId;
		}
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
	
	private boolean hasSameTypedValue(TransducerValue tValue) {
		String raw = tValue.getRawValue();
		String typed = tValue.getTypedValue();
		return raw.equals(typed);
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

}
