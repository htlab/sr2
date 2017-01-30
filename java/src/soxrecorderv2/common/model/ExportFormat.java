package soxrecorderv2.common.model;

public enum ExportFormat {
	
	CSV("csv"),
	JSON("json")
	;
	
	private String format;
	
	ExportFormat(String format) {
		this.format = format;
	}
	
	public String getFormat() {
		return format;
	}

}
