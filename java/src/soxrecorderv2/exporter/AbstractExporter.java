package soxrecorderv2.exporter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.ExportingState;
import soxrecorderv2.common.model.RecordWithValues;
import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.common.model.db.Export;
import soxrecorderv2.common.model.db.LargeObject;
import soxrecorderv2.common.model.db.Observation;
import soxrecorderv2.common.model.db.Record;
import soxrecorderv2.common.model.db.TValType;
import soxrecorderv2.common.model.db.TValue;
import soxrecorderv2.common.model.db.Transducer;
import soxrecorderv2.exporter.formatimpl.FormatExporter;
import soxrecorderv2.util.IDTuple;
import soxrecorderv2.util.MyCharSet;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SQLUtil;

public abstract class AbstractExporter implements FormatExporter {
	
	private static final String[] TYPED_VALUE_FIELDS = {
		"record_id",      // 1
		"transducer_id",  // 2
		"value_type",     // 3
		"string_value",   // 4
		"int_value",      // 5
		"decimal_value",  // 6
		"float_value",    // 7
		"large_object_id" // 8
	};
	
	private Exporter parent;
	private ExportTask task;
	private boolean isFinished = false;
	private boolean isRunning = false;
	
	public String buildHeaderContent() {
		return "";
	}
	
	public abstract String buildItemContent(Observation observation, Record record, Collection<TValue> values);

	public abstract String buildItemContent(Observation observation, Record record, Collection<TValue> values, String rawXml);
	
	public String buildFooterContent() {
		return "";
	}
	
	protected void initialize(final Exporter parent, final ExportTask task) {
		this.parent = parent;
		this.task = task;
	}
	
	public ExportTask getTask() {
		return task;
	}
	
	@Override
	public void run() {
		isRunning = true;
		isFinished = false;
		try {
			Export exportProfile = task.getExportProfile();
			Observation ob = resolveObservation(exportProfile.getId());
			boolean isUsingRaw = exportProfile.isUsingRawValue();
			
			String fileName = task.getExportProfile().getFileName();
			try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(fileName))) {
				// write header content
				byte[] headerContent = buildHeaderContent().getBytes(MyCharSet.UTF8);
				out.write(headerContent);
				
				// write records
				Connection conn = getConnManager().getConnection();
				
				String[] fields = {
					"record.id",                                 // 1
					"record.observation_id",                     // 2
					"record.is_parse_error",                     // 3
					"record.created",                            // 4
					"transducer_raw_value.has_same_typed_value", // 5
					"transducer_raw_value.transducer_id",        // 6
					"transducer_raw_value.value_type",           // 7
					"transducer_raw_value.string_value",         // 8
					"transducer_raw_value.int_value",            // 9
					"transducer_raw_value.float_value",          // 10
					"transducer_raw_value.decimal_value",        // 11
					"transducer_raw_value.large_object_id"       // 12
				};
				
				Joiner commaJoiner = Joiner.on(",");
				String joinedFields = commaJoiner.join(fields);

				List<String> conditions = new ArrayList<>();
				conditions.add("(record.id = transducer_raw_value.record_id)");
				conditions.addAll(buildTimeConditions(exportProfile));
				String joinedCond = SQLUtil.andJoin(conditions);
				
				String sql = "SELECT " + joinedFields + " FROM record, transducer_raw_value WHERE ";
				sql += joinedCond;
				sql += " ORDER BY record.created ASC;";
				
				PreparedStatement ps = conn.prepareStatement(sql);
				int argIdx = 1;
				if (!exportProfile.isFromBeginning()) {
					ps.setTimestamp(argIdx++, SQLUtil.toTimestamp(exportProfile.getTimeStart()));
				}
				if (!exportProfile.isUntilLatest()) {
					ps.setTimestamp(argIdx++, SQLUtil.toTimestamp(exportProfile.getTimeEnd()));
				}
				
				// buffering some records for efficient large_object resolve
				int nFlush = 100; // maximum buffering size
				List<RecordWithValues> valBuffer = new ArrayList<>();
				RecordWithValues tmpRec = null;
				
				// count target record with SQL to show progress
				task.setTotalRecordCount(countTotalRecord(exportProfile));
				
				ResultSet rs = ps.executeQuery();
				getConnManager().updateLastCommunicateTime();
				while (rs.next() && isRunning) {
					long rId = rs.getLong(1);
					long rObservationId = rs.getLong(2);
					boolean rIsParseError = rs.getBoolean(3);
					Timestamp rCreated = rs.getTimestamp(4);
					Record record = new Record(rId, rObservationId, rIsParseError, rCreated);
					
					if (tmpRec == null) {
						tmpRec = new RecordWithValues(record);
					} else if (!tmpRec.getRecord().equals(record)) {
						valBuffer.add(tmpRec);
						
						if (valBuffer.size() == nFlush) {
							List<RecordWithValues> content = (isUsingRaw) ? valBuffer : replaceWithTypedValues(valBuffer);
							long wrote = writeValues(out, ob, content);
							task.addProgress(wrote);
							valBuffer = new ArrayList<>();  // reset buffer
						}
						
						tmpRec = new RecordWithValues(record);
					}
					
					// transducer_raw_valueの値を入れる
					boolean hasSameTypedValue = rs.getBoolean(5);
					long transducerId = rs.getLong(6);
					int dbValType = rs.getInt(7);
					TValType valType = TValType.typeOf(dbValType);
					String stringValue = rs.getString(8);
					long intValue = rs.getLong(9);
					double floatValue = rs.getDouble(10);
					BigDecimal decimalValue = rs.getBigDecimal(11);
					long largeObjectId = rs.getLong(12);
					
					TValue value = new TValue(
						transducerId,
						valType,
						stringValue,
						intValue,
						floatValue,
						decimalValue,
						largeObjectId,
						hasSameTypedValue
					);
					tmpRec.addValue(value);
				}
				
				if (isRunning) {
					if (tmpRec != null) {
						valBuffer.add(tmpRec);
					}
					
					if (0 < valBuffer.size()) {  // more data to write
						List<RecordWithValues> content = (isUsingRaw) ? valBuffer : replaceWithTypedValues(valBuffer);
						long wrote = writeValues(out, ob, content);
						task.addProgress(wrote);
					}
					
					byte[] footerContent = buildFooterContent().getBytes(MyCharSet.UTF8);
					out.write(footerContent);

					task.setState(ExportingState.FINISHED);
					isFinished = true;  // successfully exported
				}
			}
		} catch (Exception e) {
			// TODO: logging
		} finally {
			isRunning = false;
			if (!isFinished) {
				task.setState(ExportingState.ERROR);
				// remove file if exists
				File xFile = new File(task.getExportProfile().getFileName());
				if (xFile.exists()) {
					xFile.delete();
				}
			}
		}
	}
	
	@Override
	public void stopExporting() {
		isRunning = true;
	}
	
	public PGConnectionManager getConnManager() {
		return parent.getConnManager();
	}
	
	protected List<RecordWithValues> replaceWithTypedValues(List<RecordWithValues> values) throws SQLException {
		// 1. collect idtuple (record_id, transducer_id) of not raw-value records
		List<IDTuple> resolveTarget = new ArrayList<>();
		for (RecordWithValues rec : values) {
			long recordId = rec.getRecord().getDatabaseId();
			for (TValue tValue : rec.getValues()) {
				if (!tValue.hasSameTypedValue()) {
					resolveTarget.add(new IDTuple(recordId, tValue.getTransducerId()));
				}
			}
		}
		
		// 2. resolve
		Map<IDTuple, TValue> resolvedTypedValues = resolveTypedValues(resolveTarget);
		
		// 3. replace
		List<RecordWithValues> ret = new ArrayList<>();
		for (RecordWithValues rec : values) {
			long recordId = rec.getRecord().getDatabaseId();
			List<TValue> newTdrValues = new ArrayList<>();
			
			for (TValue tValue : rec.getValues()) {
				if (tValue.hasSameTypedValue()) {
					newTdrValues.add(tValue);
				} else {
					IDTuple key = new IDTuple(recordId, tValue.getTransducerId());
					TValue typed = resolvedTypedValues.get(key);
					newTdrValues.add(typed);
				}
			}
			
			RecordWithValues newRec = new RecordWithValues(rec.getRecord(), newTdrValues);
			ret.add(newRec);
		}
		
		return ret;
	}
	
	protected Map<IDTuple, TValue> resolveTypedValues(Collection<IDTuple> ids) throws SQLException {
		Map<IDTuple, TValue> ret = new HashMap<>();
		
		Map<Long, List<Long>> groupedIds = groupByRecordId(ids);
		Map<Long, List<Long>> buffer = new HashMap<>();
		int added = 0;
		final int recordsPerSql = 50;
		for (Long recordId : groupedIds.keySet()) {
			buffer.put(recordId, groupedIds.get(recordId));
			added++;
			
			if (added == recordsPerSql) {
				// perform SQL
				Map<IDTuple, TValue> subset = resolveTypedValues(buffer);
				for (IDTuple key : subset.keySet()) {
					ret.put(key, subset.get(key));
				}
				added = 0;
				buffer.clear();
			}
		}
		
		if (0 < added) {
			Map<IDTuple, TValue> subset = resolveTypedValues(buffer);
			for (IDTuple key : subset.keySet()) {
				ret.put(key, subset.get(key));
			}
		}
		
		return ret;
	}
	
	protected Map<IDTuple, TValue> resolveTypedValues(Map<Long, List<Long>> groupedIds) throws SQLException {
		Joiner j = Joiner.on(",");
		String fields = j.join(TYPED_VALUE_FIELDS);
		
		List<String> conds = new ArrayList<>();
		List<Long> sqlArgs = new ArrayList<>();
		for (Long recordId : groupedIds.keySet()) {
			List<Long> tdrIds = groupedIds.get(recordId);
			int nTransducers = tdrIds.size();
			String cond = "record_id = ? AND transducer_id IN " + SQLUtil.buildPlaceholders(nTransducers);
			conds.add(cond);
			sqlArgs.add(recordId);
			sqlArgs.addAll(tdrIds);
		}
		
		String condition = SQLUtil.orJoin(conds);
		String sql = "SELECT " + fields + " FROM " + SR2Tables.TransducerTypedValue +  "WHERE " + condition;
		
		// query to database
		Map<IDTuple, TValue> ret = new HashMap<>();
		Connection conn = getConnManager().getConnection();
		
		PreparedStatement ps = conn.prepareStatement(sql);
		int idx = 1;
		for (Long arg : sqlArgs) {
			ps.setLong(idx++, arg);
		}
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			long recordId = rs.getLong(1);
			long transducerId = rs.getLong(2);
			
			int dbValueType = rs.getInt(3);
			TValType valType = TValType.typeOf(dbValueType);
			
			String stringValue = rs.getString(4);
			Long intValue = rs.getLong(5);
			BigDecimal decimalValue = rs.getBigDecimal(6);
			double floatValue = rs.getDouble(7);
			long largeObjectId = rs.getLong(8);
			
			TValue tVal = new TValue(
					transducerId, valType, stringValue, intValue, floatValue, decimalValue, largeObjectId, false);
			
			IDTuple idTuple = new IDTuple(recordId, transducerId);
			ret.put(idTuple, tVal);
		}
		
		rs.close();
		ps.close();
		
		return ret;
	}
	
	protected Map<Long, List<Long>> groupByRecordId(Collection<IDTuple> ids) {
		Map<Long, List<Long>> ret = new HashMap<>();
		for (IDTuple idt : ids) {
			long recordId = idt.getId1();
			long tdrId = idt.getId2();
			if (!ret.containsKey(recordId)) {
				ret.put(recordId, new ArrayList<Long>());
			}
			ret.get(recordId).add(tdrId);
		}
		return ret;
	}
	
	protected long writeValues(OutputStream out, Observation ob, List<RecordWithValues> records) throws IOException, SQLException {
		long wroteCount = 0;
		setLargeObject(records);
		for (RecordWithValues rv : records) {
			String itemContent = buildItemContent(ob, rv.getRecord(), rv.getValues());
			byte[] itemContentBytes = itemContent.getBytes(MyCharSet.UTF8);
			out.write(itemContentBytes);
			wroteCount++;
		}
		return wroteCount;
	}
	
	protected void setLargeObject(List<RecordWithValues> records) throws SQLException {
		if (records.isEmpty()) {
			return;
		}
		
		// collect large_object.id
		Set<Long> largeObjectIds = new HashSet<>();
		for (RecordWithValues rv : records) {
			List<TValue> values = rv.getValues();
			for (TValue tv : values) { 
				if (tv.getType() == TValType.LargeObject) {
					largeObjectIds.add(tv.getLargeObjectId());
				}
			}
		}
		
		// dbでlarge_objectを解決してsetする
		Map<Long, byte[]> id2loMap = getLargeObjects(largeObjectIds);  // query to db
		for (RecordWithValues rv : records) {
			List<TValue> values = rv.getValues();
			for (TValue tv : values) {
				if (tv.getType() != TValType.LargeObject) {
					continue;  // cannot happen, though...
				}
				long tvLargeObjectId = tv.getLargeObjectId();
				if (!id2loMap.containsKey(tvLargeObjectId)) {
					continue; // FIXME
				}
				byte[] content = id2loMap.get(tvLargeObjectId);
				tv.setLargeObject(content);
			}
		}
	}
	
	protected Map<Long, byte[]> getLargeObjects(Collection<Long> largeObjectIds) throws SQLException {
		Connection conn = getConnManager().getConnection();

		String placeholders = SQLUtil.buildPlaceholders(largeObjectIds.size());
		String sql = "SELECT id, is_gzipped, hash_key, content, content_length FROM large_object WHERE id IN " + placeholders + ";";
		PreparedStatement ps = conn.prepareStatement(sql);
		
		int idx = 1;
		for (Long largeObjectId : largeObjectIds) {
			ps.setLong(idx++, largeObjectId.longValue());
		}
		
		ResultSet rs = ps.executeQuery();
		getConnManager().updateLastCommunicateTime();
		Map<Long, byte[]> ret = new HashMap<>();
		while (rs.next()) {
			long loDatabaseId = rs.getLong(1);
			boolean loIsGzipped = rs.getBoolean(2);
			String loHashKey = rs.getString(3);
			byte[] loContent = rs.getBytes(4);
			long loContentLength = rs.getLong(5);
			
			if (!ret.containsKey(loDatabaseId)) {
				LargeObject lo = new LargeObject(
					loDatabaseId, loIsGzipped, loHashKey, loContent, loContentLength);
				ret.put(loDatabaseId, lo.getRealContent());  // uncompress will be performed if necessary
			}
		}
		
		return ret;
	}
	
	protected Collection<Transducer> getTransducers(long observationId) throws SQLException {
		List<Transducer> ret = new ArrayList<>();

		Connection conn = getConnManager().getConnection();
		
		String sql = "SELECT id, observation_id, name, transducer_id FROM transducer WHERE observation_id = ?;";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, observationId);
		ResultSet rs = ps.executeQuery();
		getConnManager().updateLastCommunicateTime();
		
		while (rs.next()) {
			long tDatabaseId = rs.getLong(1);
			long tObservationId = rs.getLong(2);
			String tName = rs.getString(3);
			String tTransducerId = rs.getString(4);
			
			Transducer tdr = new Transducer(tDatabaseId, tObservationId, tName, tTransducerId);
			ret.add(tdr);
		}
		
		rs.close();
		ps.close();
		
		return ret;
	}
	
	protected Map<Long, String> buildTransducerDatabaseIdToStringIdMap(Collection<Transducer> transducers) {
		Map<Long, String> ret = new HashMap<>();
		for (Transducer tdr : transducers) {
			ret.put(tdr.getDatabaseId(), tdr.getTransducerId());
		}
		return ret;
	}
	
	protected Observation resolveObservation(long observationId) throws SQLException {
		String[] fetchFields = {
			"id",                                     // 1
			"sox_server",                             // 2
			"sox_node",                               // 3
			"sox_jid",                                // 4
			"sox_password",                           // 5
			"is_using_default_user",                  // 6
			"is_existing",                            // 7
			"is_record_stopped",                      // 8
			"is_subscribed",                          // 9
			"created",                                // 10
			"is_subscribe_failed",                    // 11
			"recent_monthly_average_data_arrival",    // 12
			"recent_monthly_total_data_arrival",      // 13
			"recent_monthly_data_arrival_days"        // 14
		};
		
		Joiner j = Joiner.on(",");
		String catFields = j.join(fetchFields);
		
		String sql = "SELECT " + catFields + " FROM " + SR2Tables.Observation + " WHERE id = ?;";
		
		Connection conn = getConnManager().getConnection();
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setLong(1, observationId);
		ResultSet rs = ps.executeQuery();
		getConnManager().updateLastCommunicateTime();
		if (rs.next()) {
			Observation ret = new Observation(
				rs.getLong(1),
				rs.getString(2),
				rs.getString(3),
				rs.getString(4),
				rs.getString(5),
				rs.getBoolean(6),
				rs.getBoolean(7),
				rs.getBoolean(8),
				rs.getBoolean(9),
				rs.getTimestamp(10),
				rs.getBoolean(11),
				rs.getDouble(12),
				rs.getLong(13),
				rs.getLong(14)
			);
			
			rs.close();
			ps.close();
			return ret;
		} else {
			rs.close();
			ps.close();
			return null;
		}
	}
	
	protected long countTotalRecord(Export exportProfile) throws SQLException {
		String cond = SQLUtil.andJoin(buildTimeConditions(exportProfile));
		String sql = "SELECT COUNT(*) FROM record WHERE " + cond + ";";
		
		Connection conn = getConnManager().getConnection();
		PreparedStatement ps = conn.prepareStatement(sql);
		int idx = 1;
		if (!exportProfile.isFromBeginning()) {
			ps.setTimestamp(idx++, SQLUtil.toTimestamp(exportProfile.getTimeStart()));
		}
		if (!exportProfile.isUntilLatest()) {
			ps.setTimestamp(idx++, SQLUtil.toTimestamp(exportProfile.getTimeEnd()));
		}
		
		ResultSet rs = ps.executeQuery();
		getConnManager().updateLastCommunicateTime();
		if (!rs.next()) {
			// FIXME なにかがおかしい
		}
		
		long ret = rs.getLong(1);
		
		rs.close();
		ps.close();
		
		return ret;
	}
	
	protected List<String> buildTimeConditions(Export exportProfile) {
		List<String> ret = new ArrayList<>();
		ret.add("(record.id = transducer_raw_value.record_id)");
		if (!exportProfile.isFromBeginning()) {
			ret.add("(? <= record.created)");
		}
		if (!exportProfile.isUntilLatest()) {
			ret.add("(record.created < ?)");
		}
		return Collections.unmodifiableList(ret);
	}
	
}
