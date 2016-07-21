package soxrecorderv2.logging;

public enum SR2LogLevel {
	
	DEBUG(1), INFO(2), WARNING(3), ERROR(4);
	
	private int level;
	
	SR2LogLevel(int level) {
		this.level = level;
	}
	
	public int getLevel() {
		return level;
	}

}
