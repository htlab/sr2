package soxrecorderv2.common.model;

public class ExportTask {

	private ExportingState state;
	private final Export exportProfile;
	
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

}
