package soxrecorderv2.exporter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.ExportingState;
import soxrecorderv2.util.PGConnectionManager;

public class ExportThread implements Runnable {
	
	public static final long QUEUE_POLL_TIMEOUT_MSEC = 100;
	
	private final Exporter parent;
	private final LinkedBlockingQueue<ExportTask> taskQueue;
	private volatile boolean isRunning = false;
	private ExportTaskExecutor executor;
	
	public ExportThread(Exporter system) {
		this.parent = system;
		this.taskQueue = system.getTaskQueue();
	}
	
	public Exporter getExporter() {
		return parent;
	}
	
	public LinkedBlockingQueue<ExportTask> getTaskQueue() {
		return taskQueue;
	}
	
	@Override
	public void run() {
		isRunning = true;
		
		while (isRunning) {
			ExportTask task;
			try {
				task = taskQueue.poll(QUEUE_POLL_TIMEOUT_MSEC, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				continue;
			}
			if (task == null) {
				continue;  // null means timeout
			}
			
			task.setState(ExportingState.STARTED);
			ExportingState resultState = doExport(task);
			task.setState(resultState);
			updateStateOnDatabase(task);
		}
	}
	
	public void stopExportThread() {
		isRunning = false;
		if (executor != null) {
			executor.stopExporting();
		}
	}
	
	public ExportingState doExport(final ExportTask task) {
		try {
			executor = new ExportTaskExecutor(parent, task);
			executor.run();
			return ExportingState.FINISHED;
		} catch (Exception e) {
			return ExportingState.ERROR;
		}
	}
	
	public void updateStateOnDatabase(ExportTask task) {
		// TODO
	}
	
	public PGConnectionManager getConnManager() {
		return parent.getConnManager();
	}

}
