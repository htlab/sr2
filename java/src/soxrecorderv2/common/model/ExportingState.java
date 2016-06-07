package soxrecorderv2.common.model;

public enum ExportingState {
	
	INITIATED(1),
	IN_QUEUE(2),
	STARTED(3),
	FINISHED(4),
	EXPIRED(5),
	ERROR(-1);
	
	private final int state;
	
	ExportingState(final int state) {
		this.state = state;
	}
	
	public int getState() {
		return state;
	}

}
