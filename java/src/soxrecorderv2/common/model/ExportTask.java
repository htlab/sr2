package soxrecorderv2.common.model;

import java.util.concurrent.atomic.AtomicLong;

import soxrecorderv2.common.model.db.Export;

public class ExportTask {

	private ExportingState state;
	private final Export exportProfile;
	private AtomicLong totalRecordCount;
	private AtomicLong exportedCount;
	
	public ExportTask(ExportingState state, Export exportProfile) {
		this.state = state;
		this.exportProfile = exportProfile;
	}
	
	public void setState(ExportingState state) {
		this.state = state;
	}
	
	public ExportingState getState() {
		return state;
	}
	
	public boolean isFinished() {
		return getState() == ExportingState.FINISHED;
	}
	
	public boolean isStarted() {
		return getState() == ExportingState.STARTED;
	}
	
	public boolean isInQueue() {
		return getState() == ExportingState.IN_QUEUE;
	}
	
	public Export getExportProfile() {
		return exportProfile;
	}

	public void setTotalRecordCount(long totalRecordCount) {
		this.totalRecordCount = new AtomicLong(totalRecordCount);
	}
	
	public long getTotalRecordCount() {
		return totalRecordCount.get();
	}
	
	public void initExportedCount() {
		exportedCount = new AtomicLong();
	}
	
	public void addProgress(long progress) {
		exportedCount.addAndGet(progress);
	}
	
	public long getExportedCount() {
		return exportedCount.get();
	}

}
