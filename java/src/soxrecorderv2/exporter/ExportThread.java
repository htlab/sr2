package soxrecorderv2.exporter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.ExportingState;

public class ExportThread implements Runnable {
	
	public static final long QUEUE_POLL_TIMEOUT_MSEC = 100;
	
	private final Exporter system;
	private final LinkedBlockingQueue<ExportTask> taskQueue;
	private volatile boolean isRunning = false;
	
	public ExportThread(Exporter system) {
		this.system = system;
		this.taskQueue = system.getTaskQueue();
	}
	
	public Exporter getSystem() {
		return system;
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
			
			long exportId = task.getExportProfile().getId();
			boolean updateFinished = false;
			while (!updateFinished) {
				try {
					// update controller's export profile state.
					system.getClient().updateExportState(exportId, resultState);
					
					updateFinished = true;
				} catch (Exception e) {
					// TODO: logging
				}
			}
		}
	}
	
	public void stopExportThread() {
		isRunning = false;
	}
	
	public ExportingState doExport(final ExportTask task) {
		// TODO: implement actual exporting process
		return ExportingState.FINISHED;
	}
	

}
