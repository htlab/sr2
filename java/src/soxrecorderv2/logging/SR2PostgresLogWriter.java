package soxrecorderv2.logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import soxrecorderv2.common.model.SR2Tables;
import soxrecorderv2.recorder.Recorder;
import soxrecorderv2.recorder.RecorderSubProcess;
import soxrecorderv2.util.PGConnectionManager;
import soxrecorderv2.util.SQLUtil;
import soxrecorderv2.util.ThreadUtil;

public class SR2PostgresLogWriter implements Runnable, RecorderSubProcess {
	
	@SuppressWarnings("unused")
	private Recorder parent;
	private PGConnectionManager pgConnManager;
	private volatile boolean isRunning;
	private LinkedBlockingQueue<SR2LogItem> logItemQueue;
	private long interval;
	private final int batchSize = 50;
	
	public SR2PostgresLogWriter(Recorder parent, long interval) {
		this.parent = parent;
		this.pgConnManager = new PGConnectionManager(parent.getConfig());
		this.logItemQueue = parent.getLogItemQueue();
		this.interval = interval;
	}

	@Override
	public void shutdownSubProcess() {
		isRunning = false;
	}

	@Override
	public PGConnectionManager getConnManager() {
		return pgConnManager;
	}

	@Override
	public void run() {
		isRunning = true;
		while (isRunning) {
			long timeConsumed = 0;
			if (0 < logItemQueue.size()) {
				long tStart = System.currentTimeMillis();
				
				List<SR2LogItem> logItems = new ArrayList<>();
				while (0 < logItemQueue.size()) {
					try {
						SR2LogItem logItem = logItemQueue.take();
						logItems.add(logItem);
					} catch (InterruptedException e) {
						e.printStackTrace(); // TODO
					}
				}
//				debug("got " + logItems.size() + " log items!");
				
				if (0 < logItems.size()) {
//					debug("going to put log items into DB");
					try {
						writeLogItems(logItems);
					} catch (SQLException e) {
						e.printStackTrace();  // TODO
					}
				} else {
//					debug("zero items to insert to DB");
				}
				
				long tEnd = System.currentTimeMillis();
				timeConsumed = tEnd - tStart;
			}
			
			long sleep = interval - timeConsumed;
//			debug("going to sleep " + sleep + "msec");
			ThreadUtil.sleep(sleep);
//			debug("sleep ok");
		}
		
		pgConnManager.close();
	}
	
	/**
	 * batchSizeごとにデータをまとめて投入する。最終的にはデータを全部投入する
	 * 
	 * @param logItems
	 * @throws SQLException
	 */
	private void writeLogItems(Collection<SR2LogItem> logItems) throws SQLException {
		String[] fields = {
			"system",
			"component",
			"sox_server",
			"sox_node",
			"message",
			"log_level",
			"log_type",
			"logged_at"
		};
		String sql = SQLUtil.buildInsertSql(SR2Tables.EventLog, fields);
		Connection conn = getConnManager().getConnection();
		boolean isProblem = false;
		Savepoint sp = conn.setSavepoint();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			
			int batchCount = 0;
			for (SR2LogItem logItem : logItems) {
				logItem.fillPreparedStatement(ps);
				ps.addBatch();
				batchCount++;
				if (batchCount == batchSize) {
					ps.executeBatch();
					getConnManager().updateLastCommunicateTime();
//					System.out.println("performaed batch insert: " + batchCount);
					batchCount = 0;
				}
			}
			if (0 < batchCount) {
				ps.executeBatch();
				getConnManager().updateLastCommunicateTime();
//				System.out.println("performaed batch insert: " + batchCount);
			}
			
			ps.close();
		} catch (Exception e) {
			isProblem = true;
		} finally {
			if (isProblem) {
				conn.rollback(sp);
			} else {
				conn.commit();
			}
		}
	}

	@Override
	public String getComponentName() {
		return "log_writer";
	}
	
	public void debug(String msg) {
		System.out.println("[LogWriter] " + msg);
	}

}
